# Run this file with python3 resilient.py, to enable the migration protocol we designed
import requests
import json
import time
import yaml
import subprocess
import paramiko
import collections
import threading

from collections import deque
from datetime import datetime, timedelta
import queue
from kubernetes import client, config, utils, watch
from copy import deepcopy

class EventQueue(queue.Queue):
    def snapshot(self):
        with self.mutex:
            return list(self.queue)

class EtcdApiHandler:

	def __init__(self, url):
		# pass
		self.base_url = url

	def get(self, route, headers=None, data=None, timeout=None):

		try:
			res = requests.get(self.base_url+route, timeout=timeout)
		except requests.exceptions.ConnectionError as e:
			return {
				"status": 503,
				"error": e,
				"type": "Connection"
			}
		except requests.exceptions.RequestException as e:
			return {
				"error": e
			}
		except requests.exceptions.Timeout as e:
			return {
				"error": e,
				"type": "Timeout"
			}
		else:
			return res.json()

	def post(self, route, headers=None, data=None, timeout=None):
		try:
			res = requests.post(self.base_url+route, json=data, timeout=timeout)
		except requests.exceptions.ConnectionError as e:
			return {
				"status": 503,
				"error": e,
				"type": "Connection"
			}
		except requests.exceptions.RequestException as e:
			return {
				"error": e
			}
		except requests.exceptions.Timeout as e:
			return {
				"error": e,
				"type": "Timeout"
			}
		else:
			return res.json()

	def set_base_url(self, url):
		self.base_url = url

def get_all_nodes():
	api_response = None
	while(api_response == None):
		try:
			api_response = v1.list_node()
			# print(api_response)
		except client.rest.ApiException as e:
			print("Exception when calling list_node: %s\n" % e)
			api_response = None
		time.sleep(1)

	nodes = api_response.items

	# Enter Control Plane node here
	if not SCHEDULABLE_CONTROL_PLANE:
		nodes = list(node for node in nodes if node.metadata.name != 'kube-master')
	return nodes

def get_etcd_client_service():
	api_response = None
	while(api_response == None):
		try:
			api_response = v1.list_service_for_all_namespaces(watch=False)
			# print(api_response)
		except client.rest.ApiException as e:
			print("Exception when calling list_service_for_all_namespaces: %s\n" % e)
			api_response = None
		time.sleep(1)

	services = api_response

	# Find the etcd service for the pods
	etcd_cluster_iterator = list(filter(lambda service: \
		service.metadata.name == 'etcd-client' and \
		service.spec.type == 'LoadBalancer', \
		services.items))

	return etcd_cluster_iterator[0]

# Never try to add a pod or migrate if we see that there is a pod still initializing
def etcd_pod_being_brought_up():
	etcd_pods = get_all_etcd_pods()

	# etcd_cluster_members = requests.post(etcd_cluster_endpoint+"/v3/cluster/member/list").json()["members"]
	res = EtcdApiHandler.post("/v3/cluster/member/list", timeout=3)

	if "error" in res:
		print("Can't check if pod is being brought up right now")
		return False
	else:
		# If we improve the get_all_etcd_pods, we don't need to do the mapping to ensure the pods are part of the cluster
		etcd_cluster_members  = res["members"]
		# Pods when coming up don't have either name or clientURL
		running_etcd_members = list(member for member in etcd_cluster_members if ("name" in member) and ("clientURLs" in member))
		running_etcd_members_names = list(member["name"] for member in etcd_cluster_members if ("name" in member) and ("clientURLs" in member))  # change this iterate over to "in running_etcd_members"
		etcd_members_not_brought_up = list(member for member in etcd_cluster_members if ("name" not in member) or ("clientURLs" not in member))  # set difference
		
		print('etcd_members_not_brought_up', etcd_members_not_brought_up)

		for idx, member in enumerate(etcd_members_not_brought_up):
			delete = False
			# Etcd pod was succesfully deployed initially, but can't get up
			if "name" in member:
				pod = map_etcd_member_name_to_k8s_pod(member["name"])
				# print(member["name"], pod)
				if pod:
					# Assumes 1 container for every etcd pod
					if pod.status.container_statuses[0].restart_count > 2:
						delete = True
			if "name" not in member:
				# Assumes that there will be one other pod that isn't ready that corresponds to the nameless etcd member
				for pod in etcd_pods:
					if not pod.status.container_statuses[0].ready:
						pod_start_date = pod.metadata.creation_timestamp.date()
						pod_start_time = pod.metadata.creation_timestamp.time()

						pod_grace_start_datetime = pod.metadata.creation_timestamp + timedelta(minutes=2)

						print(pod_start_date == datetime.now().date())
						print(pod_start_date)

						# Means at least 1 day the pod has failed and still there
						if pod_start_date < datetime.now().date():
							print('Pods failed state has been active for more than a day')
							delete = True
						if pod_grace_start_datetime.time() < datetime.now().time():
							print('Pod is older than 2 minutes and is failing')
							delete = True
			if delete:
				etcd_members_not_brought_up.pop(idx)

		print('etcd members length not brought up', len(etcd_members_not_brought_up))
		return len(etcd_members_not_brought_up) > 0

def get_all_etcd_pods():
	api_response = None
	while(api_response == None):
		try:
			api_response = v1.list_pod_for_all_namespaces(watch=False)
		except client.rest.ApiException as e:
			print("Exception when calling list_pod_for_all_namespaces: %s\n" % e)
			api_response = None
		time.sleep(1)

	running_pods = api_response

	etcd_pods = list(filter(lambda pod: \
		'app' in pod.metadata.labels and \
		pod.metadata.labels['app'] == 'etcd', \
		running_pods.items)
	)

	return etcd_pods

def get_available_nodes_from_nodes(nodes):
	if len(nodes) == 0: return []
	return list(node for node in nodes if node.status.conditions[-1].message != 'Kubelet stopped posting node status.')

def create_etcd_pod(new_etcd_name, new_pv_name, domain, rack):

	def create_pv(pv_name):
		# PersistentVolume
		metadata = client.V1ObjectMeta(name=pv_name, labels={"type": "local"})
		spec = client.V1PersistentVolumeSpec(storage_class_name="manual", capacity={"storage": "2Gi"}, access_modes=["ReadWriteOnce"], persistent_volume_reclaim_policy="Retain", host_path=client.V1HostPathVolumeSource(path="/root/data/myss"))
		persistent_volume_data = client.V1PersistentVolume(api_version="v1", kind="PersistentVolume", metadata=metadata, spec=spec)

		metadata_name = pv_name
		metadata_labels = {"type": "local"}
		metadata = client.V1ObjectMeta(name=metadata_name, labels=metadata_labels)

		spec_storage_class_name="manual"
		spec_capacity={"storage": "2Gi"}
		spec_access_modes=["ReadWriteOnce"]
		spec_persistent_volume_reclaim_policy="Retain"
		spec_host_path=client.V1HostPathVolumeSource(path="/root/data/myss")
		spec = client.V1PersistentVolumeSpec(storage_class_name=spec_storage_class_name, capacity=spec_capacity, access_modes=spec_access_modes, persistent_volume_reclaim_policy=spec_persistent_volume_reclaim_policy, host_path=spec_host_path)

		pv_api_version = "v1"
		pv_kind = "PersistentVolume"
		persistent_volume_data = client.V1PersistentVolume(api_version=pv_api_version, kind=pv_kind, metadata=metadata, spec=spec)

		api_response = None
		while(api_response == None):
			try:
				api_response = v1.create_persistent_volume(persistent_volume_data)
				# print(api_response)
			except client.rest.ApiException as e:
				print("Exception when calling create_persistent_volume: %s\n" % e)
				api_response = None
			time.sleep(1)

	def create_statefulset(etcd_host_name):
		# StatefulSet and PVC

		# statefulset.apiVersion
		ss_api_version = "apps/v1"
		# statefulset.kind
		ss_kind = "StatefulSet"

		# statefulset.metadata
		ss_metadata_name = etcd_host_name
		ss_metadata_labels = {"app": "etcd"}
		ss_metadata = client.V1ObjectMeta(name=ss_metadata_name, labels=ss_metadata_labels)

		# statefulset.spec
		ss_spec_service_name = "etcd"

		ss_spec_selector_match_labels = {"app": "etcd"}
		ss_spec_selector = client.V1LabelSelector(match_labels=ss_spec_selector_match_labels)

		ss_spec_replicas = 1

		# statefulset.spec.template
		# statefulset.spec.template.metadata
		ss_spec_template_metadata_name = "etcd"
		ss_spec_template_metadata_labels = {"app": "etcd"}

		ss_spec_template_metadata = client.V1ObjectMeta(name=ss_spec_template_metadata_name, labels=ss_spec_template_metadata_labels)

		# statefulset.spec.template.spec
		# statefulset.spec.template.spec.affinity
		affinity = None

		key = 'domain'
		operator = 'In'
		values = [domain]
		node_selector_requirement_domain = client.V1NodeSelectorRequirement(key=key, operator=operator, values=values)

		key = 'rack'
		operator = 'In'
		values = [rack]
		node_selector_requirement_rack = client.V1NodeSelectorRequirement(key=key, operator=operator, values=values)

		node_selector_terms = [client.V1NodeSelectorTerm(match_expressions=[node_selector_requirement_domain, node_selector_requirement_rack])]
		node_selector = client.V1NodeSelector(node_selector_terms=node_selector_terms)
		node_affinity = client.V1NodeAffinity(required_during_scheduling_ignored_during_execution=node_selector) # Use preferred later on because we might still want to schedule the pod anyways?
		affinity = client.V1Affinity(node_affinity=node_affinity)

		# statefulset.spec.template.spec.containers
		ss_spec_template_spec_containers = []
		container_name = etcd_host_name
		container_image = "quay.io/coreos/etcd:v3.5.3"

		container_ports = []

		client_port_container_port = 2379
		client_port_name = "client"
		client_port = client.V1ContainerPort(container_port=client_port_container_port, name=client_port_name)

		peer_port_container_port = 2380
		peer_port_name = "peer"
		peer_port = client.V1ContainerPort(container_port=peer_port_container_port, name=peer_port_name)

		container_ports.append(client_port)
		container_ports.append(peer_port)

		container_volume_mounts = []
		volume_mount_name = "task-pvc-volume"
		volume_mount_mount_path = "/root/data/myss"
		volume_mount = client.V1VolumeMount(name=volume_mount_name,mount_path=volume_mount_mount_path)
		container_volume_mounts.append(volume_mount)

		print(get_current_initial_cluster() + f'{etcd_host_name}-0=http://{etcd_host_name}-0.etcd:2380 ')

		# Initial clluster, ports, everything is hard-coded here, Make it dynamic?
		bin_cmd = '/bin/sh'
		c = '-c'
		# Not actually using this variable in the command
		peers = f'PEERS="{etcd_host_name}-0=http://{etcd_host_name}-0.etcd:2380,etcd1-0=http://etcd1-0.etcd:2380,etcd0-0=http://etcd0-0.etcd:2380"'
		exec_etcd = f'exec etcd --name {etcd_host_name}-0'
		listen_peer = '  --listen-peer-urls http://0.0.0.0:2380 '
		listen_client = '  --listen-client-urls http://0.0.0.0:2379 '
		adv = f'  --advertise-client-urls http://{etcd_host_name}-0.etcd:2379 '
		init_adv = f'  --initial-advertise-peer-urls http://{etcd_host_name}-0:2380 '
		init_tok = '  --initial-cluster-token etcd-cluster-1604 '
		init_clust = f'  --initial-cluster {get_current_initial_cluster()}{etcd_host_name}-0=http://{etcd_host_name}-0.etcd:2380 '
		init_state = '  --initial-cluster-state existing '
		data_dir = '  --data-dir /root/data/myss '

		container_command = [bin_cmd, c, f'{peers}\n{exec_etcd} \\\n{listen_peer} \\\n{listen_client} \\\n{adv} \\\n{init_adv} \\\n{init_tok} \\\n{init_clust} \\\n{init_state} \\\n{data_dir}\n']

		container = client.V1Container(name=container_name, image=container_image, ports=container_ports, volume_mounts=container_volume_mounts, command=container_command)

		ss_spec_template_spec_containers.append(container)

		ss_spec_template_spec = client.V1PodSpec(affinity=affinity, containers=ss_spec_template_spec_containers)


		ss_spec_template = client.V1PodTemplateSpec(metadata=ss_spec_template_metadata,spec=ss_spec_template_spec)

		# statefulset.spec.volume_claim_templates
		volume_claim_template_metadata_name = "task-pvc-volume"
		volume_claim_template_metadata = client.V1ObjectMeta(name=volume_claim_template_metadata_name)
		volume_claim_template_spec_access_modes = ["ReadWriteOnce"]
		volume_claim_template_spec_storage_class_name = "manual"
		volume_claim_template_spec_resources_requests = {"storage": "1Gi"}
		volume_claim_template_spec_resources = client.V1ResourceRequirements(requests=volume_claim_template_spec_resources_requests)
		volume_claim_template_spec = client.V1PersistentVolumeClaimSpec(access_modes=volume_claim_template_spec_access_modes, storage_class_name=volume_claim_template_spec_storage_class_name, resources=volume_claim_template_spec_resources)
		volume_claim_templates = []
		volume_claim_templates.append(client.V1PersistentVolumeClaim(metadata=volume_claim_template_metadata, spec=volume_claim_template_spec))

		ss_spec = client.V1StatefulSetSpec(service_name=ss_spec_service_name,selector=ss_spec_selector,replicas=ss_spec_replicas,template=ss_spec_template,volume_claim_templates=volume_claim_templates)


		statefulset = client.V1StatefulSet(api_version=ss_api_version, kind=ss_kind, metadata=ss_metadata, spec=ss_spec)

		api_response = None
		while(api_response == None):
			try:
				api_response = appsV1.create_namespaced_stateful_set(namespace="default", body=statefulset)
			except client.rest.ApiException as e:
				print("Exception when calling AppsV1Api->create_namespaced_stateful_set: %s\n" % e)
				api_response = None
			time.sleep(1)

	create_pv(new_pv_name)
	time.sleep(3)

	print('creating this pod', new_etcd_name, new_pv_name, domain)

	etcd_host_name = new_etcd_name
	# We assume we only bring up 1 replica for etcd pod statefulset, so "-0"
	add_etcd_member_to_cluster(f"http://{etcd_host_name}-0.etcd:2380")
	create_statefulset(etcd_host_name)

def get_current_initial_cluster():
	res = EtcdApiHandler.post("/v3/cluster/member/list", timeout=2)

	if "error" in res:
		while "error" in res:
			res = EtcdApiHandler.post("/v3/cluster/member/list", timeout=2)
			time.sleep(1)

	initial_cluster = ''
	for member in res["members"]:
		# Assuming 1 peer URL per member
		if "name" in member: # Some peerURLs may exist without a name because the pod isn't brought up yet. Really only one member would be in this state (since migration is done once at a time)
			initial_cluster += f'{member["name"]}={member["peerURLs"][0]},'
	return initial_cluster

def add_etcd_member_to_cluster(peer_urls):
	print("The peer url", peer_urls)
	data = {"peerURLs": [peer_urls]}

	res = requests.post(etcd_cluster_endpoint+"/v3/cluster/member/add", json=data).json()
	if "error" in res:
		while "error" in res:
			print("Error adding etcd peerURL to cluster")
			print(res)
			res = requests.post(etcd_cluster_endpoint+"/v3/cluster/member/add", json=data).json()
			time.sleep(1)

	print('added member', res)

def remove_etcd_member(etcd_member_name, remove_data_dir=None):

	res = map_etcd_name_to_member_details(etcd_member_name)

	member_details = res

	data = {"ID": member_details["ID"]}

	print('will remove this member', member_details["ID"])
	res = EtcdApiHandler.post("/v3/cluster/member/remove", data=data, timeout=2)

	if "error" in res:
		print("Error trying to remove member from cluster")
		while "error" in res:
			res = EtcdApiHandler.post("/v3/cluster/member/remove", data=data, timeout=2)
			time.sleep(1)

	# Remove the replica labeled index
	statefulset_name = etcd_member_name[:-2]

	print('deleting the statefulset:', statefulset_name)

	# Deletes statefulset.apps/<etcd name without replica label>
	api_response = None
	while(api_response == None):
		try:
			api_response = appsV1.delete_namespaced_stateful_set(name=statefulset_name, namespace="default")
			# print(api_response)
		except client.rest.ApiException as e:
			print("Exception when calling AppsV1Api->create_namespaced_stateful_set: %s\n" % e)
			api_response = None
		time.sleep(1)

	local_etcd_monitor.pop(etcd_member_name, None)

	if(remove_data_dir):
		print('SSH process to delete')

def map_etcd_member_name_to_k8s_pod(etcd_name):
	etcd_pods = get_all_etcd_pods()
	for etcd_pod in etcd_pods:
		if etcd_pod.metadata.name == etcd_name:
			return etcd_pod
	return None


# Assumes with Replica Index
def map_etcd_name_to_member_details(etcd_name):
	res = EtcdApiHandler.post("/v3/cluster/member/list", timeout=2)
	if "error" in res:
		print("Error trying to map to member details in remove_etcd_member")
		while "error" in res:
			time.sleep(1)
			res = EtcdApiHandler.post("/v3/cluster/member/list", timeout=2)

	for member in res["members"]:
		if ("name" in member) and (member["name"] == etcd_name):
			return member
	# We assume that the etcd_name being passed in will always have a valid mapping to the member list
	# return None

def map_node_names_to_node_objects(node_names):
	nodes = get_all_nodes()
	return [n for n in nodes if n.metadata.name in node_names]

def map_node_name_to_node_object(node_name):
	nodes = get_all_nodes()
	return [n for n in nodes if n.metadata.name == node_name][0]

def get_unique_etcd_name(etcd_id): # (etcd_counter_id): Parameter will not overwrite the global variable
	# Attaching the indexing done
	return f"etcd{etcd_id}", f"etcd{etcd_id}-0"

def get_unique_pv_name(pv_id):
	# Attaching the indexing done
	return f"task-pv-volume-{pv_id}"

def data_directory_exists(node):
	pass
	# Whenever a node is made available, check that is has a valid data directory pathing for the PV

def remove_stale_data_directories():
	# We need to check if node is first available, if it is then delete it
	# If we schedule an etcd pod, we need to make sure that the node does not have a data directory active.
	# ssh.connect(server, username='ronch', password=password)
	# ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd_to_execute)
	pass

def get_domain_structure_abstraction():

	read_get_lock.acquire()

	domains = []

	# Determine which domain needs the etcd pod
	# Return that label which will be used in the statefulset creation
	nodes = get_all_nodes()
	for node in nodes:
		# If node isn't part of a domain, we don't consider it as a possible node to host an etcd pod
		if 'domain' in node.metadata.labels:
			domain_name = node.metadata.labels['domain']
			# print('line 443', domains)
			if (not any(domain['name'] == domain_name for domain in domains)):
				domain = {}
				domain["name"] = domain_name
				domain["node_objects"] = []
				domain["node_names"] = []
				domain["num_nodes"] = 0
				domain["num_etcd_pods"] = 0
				domain["etcd_pod_names"] = []
				domain["pending_num_etcd_removals"] = 0
				domain["pending_num_etcd_additions"] = 0
				domain["pending_etcd_removals"] = []
				domain["pending_etcd_additions"] = []
				domain["racks"] = [] # Basically the same structure as domains
				domains.append(domain)

			for idx, domain in enumerate(domains):
				if domain["name"] == domain_name:
					domains[idx]["node_objects"].append(node)
					domains[idx]["node_names"].append(node.metadata.name)
					domains[idx]["num_nodes"] += 1
					# Putting it here would assume we count nodes in a domain but if they don't have a rack label, we still count them, just not adding them to our rack
					if 'rack' in node.metadata.labels:
						rack_name = node.metadata.labels['rack']
						if (not any(rack['name'] == rack_name for rack in domains[idx]["racks"])):
							rack = {}
							rack["name"] = rack_name
							rack["node_objects"] = []
							rack["node_names"] = []
							rack["num_nodes"] = 0
							rack["num_etcd_pods"] = 0
							rack["etcd_pod_names"] = []
							rack["pending_num_etcd_removals"] = 0
							rack["pending_num_etcd_additions"] = 0
							rack["pending_etcd_removals"] = []
							rack["pending_etcd_additions"] = []
							domains[idx]["racks"].append(rack)

						for rack_idx, rack in enumerate(domains[idx]["racks"]):
							if rack["name"] == rack_name:
								domains[idx]["racks"][rack_idx]["node_objects"].append(node)
								domains[idx]["racks"][rack_idx]["node_names"].append(node.metadata.name)
								domains[idx]["racks"][rack_idx]["num_nodes"] += 1

	etcd_pods = get_all_etcd_pods()

	for etcd_pod in etcd_pods:
		for idx, domain in enumerate(domains):
			if etcd_pod.spec.node_name in domain["node_names"]:
				domains[idx]["num_etcd_pods"] += 1
				domains[idx]["etcd_pod_names"].append(etcd_pod.metadata.name)
 
				for rack_idx, rack in enumerate(domains[idx]["racks"]):
					if etcd_pod.spec.node_name in rack["node_names"]:
						domains[idx]["racks"][rack_idx]["num_etcd_pods"] += 1
						domains[idx]["racks"][rack_idx]["etcd_pod_names"].append(etcd_pod.metadata.name)

	q = get_queue()

	for event in q.snapshot():
		event_type = event[2]
		event_metadata = event[3]
		print(event_metadata)

		if event_type == 'MIGRATION':
			etcd_member_being_deleted = event_metadata['OLD_ETCD_MEMBER']
			etcd_pod_being_deleted = map_etcd_member_name_to_k8s_pod(etcd_member_being_deleted)
			node_name_holding_deleted_etcd_pod = etcd_pod_being_deleted.spec.node_name

			new_domain_holding_pod = event_metadata['NEW_DOMAIN']

			for idx, domain in enumerate(domains):
				if node_name_holding_deleted_etcd_pod in domain["node_names"]:
					domains[idx]["pending_num_etcd_removals"] -= 1
					domains[idx]["pending_etcd_removals"].append(etcd_member_being_deleted)

					rack_idx = [rack_idx for rack_idx, rack in enumerate(domains[idx]["racks"]) if node_name_holding_deleted_etcd_pod in rack["node_names"]][0]

					domains[idx]["racks"][rack_idx]["pending_num_etcd_removals"] -= 1
					domains[idx]["racks"][rack_idx]["pending_etcd_removals"].append(etcd_member_being_deleted)

				if new_domain_holding_pod == domain["name"]:
					domains[idx]["pending_num_etcd_additions"] += 1
					domains[idx]["pending_etcd_additions"].append(event_metadata['NEW_ETCD_MEMBER_NAME_INDEXED'])

					new_rack_holding_pod = event_metadata['NEW_RACK']

					rack_idx = [rack_idx for rack_idx, rack in enumerate(domains[idx]["racks"]) if new_rack_holding_pod == rack["name"]][0]

					domains[idx]["racks"][rack_idx]["pending_num_etcd_additions"] += 1
					domains[idx]["racks"][rack_idx]["pending_etcd_additions"].append(event_metadata['NEW_ETCD_MEMBER_NAME_INDEXED'])

		elif event_type == 'CREATE':
			print('the metadata for the create event', event_metadata)

			new_domain_holding_pod = event_metadata['NEW_DOMAIN']

			for idx, domain in enumerate(domains):
				if new_domain_holding_pod == domain["name"]:
					domains[idx]["pending_num_etcd_additions"] += 1
					domains[idx]["pending_etcd_additions"].append(event_metadata['NEW_ETCD_MEMBER_NAME_INDEXED'])

					new_rack_holding_pod = event_metadata['NEW_RACK']

					rack_idx = [rack_idx for rack_idx, rack in enumerate(domains[idx]["racks"]) if new_rack_holding_pod == rack["name"]][0]

					domains[idx]["racks"][rack_idx]["pending_num_etcd_additions"] += 1
					domains[idx]["racks"][rack_idx]["pending_etcd_additions"].append(event_metadata['NEW_ETCD_MEMBER_NAME_INDEXED'])



	for idx, _ in enumerate(domains):
		d = domains[idx]
		domains[idx]["num_etcd_pods_after_pending_changes"] = d["num_etcd_pods"]+d["pending_num_etcd_removals"]+d["pending_num_etcd_additions"]
		for rack_idx, rack in enumerate(domains[idx]["racks"]):
			domains[idx]["racks"][rack_idx]["num_etcd_pods_after_pending_changes"] = rack["num_etcd_pods"]+rack["pending_num_etcd_removals"]+rack["pending_num_etcd_additions"]

	# Domains with more pods are first, this will make comparing with the ideal_spread easier
	domains.sort(key=lambda d: d["num_etcd_pods_after_pending_changes"], reverse=True)

	read_get_lock.release()

	return domains

# This uses a unique migraiton where we get the spread diffs. This will solve the problem of migration due to domain/node additions
def update_cluster():
	# If we are already reaching FTT and there is an addition of nodes and domains, we do migration only
	global pv_counter_id, etcd_counter_id

	num_pods_to_add = check_pod_and_node_failure_tolerance()
	if num_pods_to_add > 0:
		for i in range(num_pods_to_add):
			new_etcd_name, new_etcd_name_indexed = get_unique_etcd_name(etcd_counter_id)
			new_pv_name = get_unique_pv_name(pv_counter_id)
			new_domain = get_domain_label()
			new_rack = get_rack_label(new_domain)
			if new_domain and new_rack:
				event_metadata = {
					"NEW_DOMAIN": new_domain,
					"NEW_RACK": new_rack,
					"NEW_ETCD_MEMBER_NAME": new_etcd_name,
					"NEW_ETCD_MEMBER_NAME_INDEXED": new_etcd_name_indexed,
					"NEW_PV_NAME": new_pv_name
				}
				queue_migration_events(funcs=[create_etcd_pod], args=[(new_etcd_name, new_pv_name, new_domain, new_rack)], types=['CREATE'], events_metadata=[event_metadata])
				pv_counter_id += 1
				etcd_counter_id += 1

	time.sleep(2)

	etcd_spread_changes = check_etcd_spread_across_domains()

	domains = get_domain_structure_abstraction()

	etcd_members_to_migrate = list()
	domains_to_hold_pods = list()

	if etcd_spread_changes:
		for domain_idx, num_etcd_displacement in enumerate(etcd_spread_changes):
			domain = domains[domain_idx]
			if num_etcd_displacement < 0:
				num_pods_to_migrate = abs(num_etcd_displacement)
				possible_etcd_candidates = list(set(domain["etcd_pod_names"]) - set(domain["pending_etcd_removals"]))
				print('possible_etcd_candidates', possible_etcd_candidates)
				etcd_members_to_migrate += possible_etcd_candidates[:num_pods_to_migrate]
				print('etcd_members_to_migrate', etcd_members_to_migrate)
			elif num_etcd_displacement > 0:

				possible_nodes = domain["node_objects"]
				number_of_available_nodes = len(get_available_nodes_from_nodes(possible_nodes)) - (domain["num_etcd_pods_after_pending_changes"])

				if number_of_available_nodes >= num_etcd_displacement:
					domains_to_hold_pods += num_etcd_displacement * [domains[domain_idx]["name"]]
					print(domains_to_hold_pods)
				else:
					print('Migrate as many pods as we can to support this domain')
					domains_to_hold_pods += number_of_available_nodes * [domains[domain_idx]["name"]]
					print('Add more nodes to this domain so that we can add more pods to support the ideal spread')
			else:
				print('No pods from this domain need to be moved')

	if len(etcd_members_to_migrate) > len(domains_to_hold_pods):
		num_members_to_exclude = len(etcd_members_to_migrate) - len(domains_to_hold_pods)
		etcd_members_to_migrate = etcd_members_to_migrate[:-num_members_to_exclude or None]

	for etcd_member, new_domain in zip(etcd_members_to_migrate, domains_to_hold_pods):
		print('etcd_member', etcd_member)
		new_etcd_name, new_etcd_name_indexed = get_unique_etcd_name(etcd_counter_id)
		new_pv_name = get_unique_pv_name(pv_counter_id)
		new_rack = get_rack_label(new_domain)
		if new_domain and new_rack:
			event_metadata = {
				"OLD_ETCD_MEMBER": etcd_member,
				"NEW_DOMAIN": new_domain,
				"NEW_RACK": new_rack,
				"NEW_ETCD_MEMBER_NAME": new_etcd_name,
				"NEW_ETCD_MEMBER_NAME_INDEXED": new_etcd_name_indexed,
				"NEW_PV_NAME": new_pv_name
			}
			print(new_domain)
			queue_migration_events(funcs=[migration_via_removal_add], args=[(etcd_member, new_etcd_name, new_pv_name, new_domain, new_rack)], types=['MIGRATION'], events_metadata=[event_metadata])
			pv_counter_id += 1
			etcd_counter_id += 1

		print(etcd_member, new_domain)

def get_ideal_etcd_spread_across_domains(domains):
	# Domains will already be sorted from highest etcd_pods + pending_etcd_pods
	num_total_etcd_pods = 0
	for domain in domains:
		num_total_etcd_pods += domain["num_etcd_pods_after_pending_changes"]

	num_domains = len(domains)
	ideal_spread = [0] * num_domains
	for i in range(num_total_etcd_pods):
		ideal_spread[i%num_domains] += 1
	return ideal_spread

def get_current_etcd_spread_across_domains(domains):
	current_spread = []
	for domain in domains:
		current_spread.append(domain["num_etcd_pods_after_pending_changes"])
	return current_spread

# TODO: Rename to something more relevant, i.e. get_etcd_displacement_for_domains
def check_etcd_spread_across_domains():

	domains = get_domain_structure_abstraction()
	etcd_pods = get_all_etcd_pods()

	ideal_spread = get_ideal_etcd_spread_across_domains(domains)
	print('ideal', ideal_spread)

	current_spread = get_current_etcd_spread_across_domains(domains)
	print('current', current_spread)

	if ideal_spread == current_spread:
		return None
	elif collections.Counter(ideal_spread) == collections.Counter(current_spread):
		return None

	else:
		spread_difference = list()
		for pods1, pods2 in zip(current_spread, ideal_spread):
			spread_difference.append(pods2 - pods1)

		print('spread diff', spread_difference)
		return spread_difference

def check_pod_and_node_failure_tolerance():
	nodes = v1.list_node().items #TODO : change this to get_all_nodes()
	num_nodes = len(nodes)

	etcd_pods = get_all_etcd_pods()
	num_etcd_pods = len(etcd_pods)

	if num_nodes >= (2*FTT)+1 and num_etcd_pods < (2*FTT)+1:
		num_etcd_pods_to_bring_up = ((2*FTT)+1) - num_etcd_pods
		return num_etcd_pods_to_bring_up
	elif num_nodes < (2*FTT)+1:
		print('we need more nodes')
		# Nothing the script can do until more nodes are added
		return 0
	else:
		# We are reaching FTT
		return 0

def get_rack_label(domain_name):

	if not domain_name:
		return None

	domains = get_domain_structure_abstraction()
	domain = [d for d in domains if d["name"] == domain_name][0]

	rack_label = None

	racks = domain["racks"]
	racks.sort(key=lambda r: r["num_etcd_pods_after_pending_changes"])

	for rack in racks:
		possible_nodes = rack["node_objects"]
		number_of_available_nodes = len(get_available_nodes_from_nodes(possible_nodes)) - (rack["num_etcd_pods_after_pending_changes"])

		# Takes into account the upcoming free node of the queued migration event
		if number_of_available_nodes > 0:
			rack_label = rack["name"]
			break

	print('the rack chosen:', rack_label)
	return rack_label

def get_new_migration_host(etcd_pod, domain_it_belongs_to):

	def determine_new_host(domain_idx):
		possible_domain = domains[domain_idx]
		possible_nodes = possible_domain["node_objects"]
		number_of_available_nodes = len(get_available_nodes_from_nodes(possible_nodes)) - (possible_domain["num_etcd_pods_after_pending_changes"])

		if number_of_available_nodes > 0:
			return possible_domain["name"]
		else:
			return None

	print('Etcd pod to be migrated:', etcd_pod.metadata.name)
	print('Its domain', domain_it_belongs_to)

	domains = get_domain_structure_abstraction()
	current_etcd_spread = get_current_etcd_spread_across_domains(domains)
	print('current_etcd_spread', current_etcd_spread)

	available_nodes = None
	possible_domain = None
	num_pods_in_current_domain = None

	# TODO: Assumes a Domain exists
	domain_idx_for_current_pod = next(idx for idx, domain in enumerate(domains) if domain["name"] == domain_it_belongs_to)
	new_domain_name = determine_new_host(domain_idx_for_current_pod)

	if not new_domain_name:
		modified_etcd_spread = current_etcd_spread
		print(modified_etcd_spread)

		for index, num_etcd in reversed(list(enumerate(modified_etcd_spread))):
			if index != domain_idx_for_current_pod:
				new_domain_name = determine_new_host(index)
				if new_domain_name:
					break

	print(new_domain_name)

	return new_domain_name

# I think we should separate out the logic for label when bringing up a pod, seems more practical that way
# and because the above needs to have the etcd_pod as a parameter to determine what domain it is in, and this is completely irrelevant for finding a label to bring up a brand new pod
def get_domain_label():

	domains = get_domain_structure_abstraction()
	current_etcd_spread = get_current_etcd_spread_across_domains(domains)

	print(current_etcd_spread)

	new_domain_name = None

	for index, num_etcd in reversed(list(enumerate(current_etcd_spread))):
		possible_domain = domains[index]
		possible_nodes = possible_domain["node_objects"]
		# if too risky of assumption, we can get the list of nodes holding the pods
		number_of_available_nodes = len(get_available_nodes_from_nodes(possible_nodes)) - (possible_domain["num_etcd_pods_after_pending_changes"])

		# Guess this takes into account the upcoming free node of the queued migration event
		if number_of_available_nodes > 0:
			new_domain_name = possible_domain["name"]
			break

	print(new_domain_name)
	return new_domain_name

def migration_via_removal_add(etcd_member_name_to_be_removed, new_etcd_name, new_pv_name, domain, rack):
	# This new_etcd_name should not have the index attached.
	remove_etcd_member(etcd_member_name_to_be_removed)
	create_etcd_pod(new_etcd_name, new_pv_name, domain, rack)


# ---
# Deef_diff of two dictionaries function
# Credit: Zephaniah Grunschlag on StackOverflow
def deep_diff(x, y, parent_key=None, exclude_keys=[], epsilon_keys=[]):
        """
        Take the deep diff of JSON-like dictionaries

        No warranties when keys, or values are None

        """
        # pylint: disable=unidiomatic-typecheck

        EPSILON = 0.5
        rho = 1 - EPSILON

        if x == y:
            return None

        if parent_key in epsilon_keys:
            xfl, yfl = float_or_None(x), float_or_None(y)
            if xfl and yfl and xfl * yfl >= 0 and rho * xfl <= yfl and rho * yfl <= xfl:
                return None

        if not (isinstance(x, (list, dict)) and (isinstance(x, type(y)) or isinstance(y, type(x)))):
            return x, y

        if isinstance(x, dict):
            d = type(x)()  # handles OrderedDict's as well
            for k in x.keys() ^ y.keys():
                if k in exclude_keys:
                    continue
                if k in x:
                    d[k] = (deepcopy(x[k]), None)
                else:
                    d[k] = (None, deepcopy(y[k]))

            for k in x.keys() & y.keys():
                if k in exclude_keys:
                    continue

                next_d = deep_diff(x[k], y[k], parent_key=k, exclude_keys=exclude_keys, epsilon_keys=epsilon_keys)
                if next_d is None:
                    continue

                d[k] = next_d

            return d if d else None

        # assume a list:
        d = [None] * max(len(x), len(y))
        flipped = False
        if len(x) > len(y):
            flipped = True
            x, y = y, x

        for i, x_val in enumerate(x):
            d[i] = (
                    deep_diff(y[i], x_val, parent_key=i, exclude_keys=exclude_keys, epsilon_keys=epsilon_keys
                        )
                    if flipped
                    else deep_diff(
                        x_val, y[i], parent_key=i, exclude_keys=exclude_keys, epsilon_keys=epsilon_keys
                        )
                    )

        for i in range(len(x), len(y)):
            d[i] = (y[i], None) if flipped else (None, y[i])

        return None if all(map(lambda x: x is None, d)) else d

# Infinite watch stream on Kubernetes cluster
def watch_nodes():
    resource_version = None

    monitoring_nodes = []
    node_events = {}

    watch1 = watch.Watch()
    iters = 0
    min_passed = False
    start = time.time()

    for event in watch1.stream(func=v1.list_node):
        print("%s %s %s %s %s %s %s" % (event["object"].kind, event["object"].metadata.name, event["type"], event["object"].status.addresses[0].address, event["object"].status.conditions[4].type, event["object"].status.conditions[4].status, event["object"].status.conditions[4].reason), flush=True)
        print("---")

        # TODO: Maybe some try statement to catch errors

        node_name = event["object"].metadata.name

        if not min_passed:
            now = time.time()
            if start+60 <= now or event["type"] == "MODIFIED":
                min_passed = True
        # print('Min passed: ', min_passed)

        if event["type"] == "ADDED":
            # print('Event: ', event)
            new_event = event["object"].to_dict()
            if min_passed:
                print('New worker: ', node_name)

                # print('1 min has passed')
            if node_name not in monitoring_nodes:
                # TODO: Deal with initial node addition because our array is empty
                monitoring_nodes.append(node_name)
                node_events[node_name] = new_event
                # get_domain_structure_abstraction()
                # change it to where we update and get differently
                # update_cluster()

            metadata = new_event.get('metadata')
            if metadata:
                labels = metadata.get('labels')
                if labels:
                    domain_changed = labels.get('domain')
                    rack_changed = labels.get('rack')
                    if domain_changed:
                        print('Domain', domain_changed)
                    elif rack_changed:
                        print('Rack', rack_changed)
                    elif rack_changed and domain_changed:
                        print('Domain', domain_changed)
                        print('Rack', rack_changed)
                    else:
                        continue

        elif event["type"] == "MODIFIED":
            new_event = event["object"].to_dict()
            diff = deep_diff(node_events[node_name], new_event)
            # print('Difference: ', diff)
            node_events[node_name] = new_event
            metadata = diff.get('metadata')
            if metadata:
                labels = metadata.get('labels')
                if labels:
                    domain_changed = labels.get('domain')
                    rack_changed = labels.get('rack')
                    if domain_changed:
                        if domain_changed[0] == None:
                            print('Domain added')
                        elif domain_changed[1] == None:
                            print('Domain removed')
                        else:
                            print('Domain modified')
                    elif rack_changed:
                        if rack_changed[0] == None:
                            print('Rack added')
                        elif rack_changed[1] == None:
                            print('Rack removed')
                        else:
                            print('Rack modified')
                    elif rack_changed and domain_changed:
                        if rack_changed[0] == None and domain_changed[0] == None:
                            print('Rack and domain added')
                        elif rack_changed[1] == None and domain_changed[1] == None:
                            print('Rack and domain removed')
                        else:
                            print('Rack and domain modified')
                    else:
                        continue
            #for diff in list(dictdiffer.diff(node_events[node_name], event["object"])):
            #    print('Difference is: ', diff)
            #print('What we have now: ', node_events[node_name], '\n\n\n\n\n\n\n\n\n\n\n\n')
# ---


def get_connection(ip, log, paswd):
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(ip, username=log, password=paswd, timeout=100)
    console = ssh.invoke_shell()
    console.keep_this = ssh
    print(console)
    return console

def check_node_details():
	api = client.CustomObjectsApi()
	print(api.list_cluster_custom_object(group="metrics.k8s.io", version="v1beta1", plural="nodes"))
	print('---')
	print(api.list_cluster_custom_object(group="metrics.k8s.io", version="v1beta1", plural="pods"))

def etcd_health_monitoring():

	# This basically accounts for the Node itself not working
	def check_etcd_pod_responsiveness():
		etcd_pods = get_all_etcd_pods()
		res = EtcdApiHandler.post("/v3/cluster/member/list", timeout=5)
		if ("error" in res):
			print(res)
			print("Skipping this round of etcd health checks due to request not going through")
			return []

		etcd_cluster_members = res["members"]
		
		running_etcd_members = list(member for member in etcd_cluster_members if ("name" in member) and ("clientURLs" in member))
		running_etcd_members_names = list(member["name"] for member in etcd_cluster_members if ("name" in member) and ("clientURLs" in member))
		etcd_members_not_brought_up = list(member for member in etcd_cluster_members if ("name" not in member) or ("clientURLs" not in member))

		print('established members', running_etcd_members)
		print('not broughtup members', etcd_members_not_brought_up)

		# Map etcd members onto the k8s pods. We will only do health checks on etcd pods that are being tracked by the cluster and are in a running state
		functioning_etcd_pods = [etcd_pod for etcd_pod in etcd_pods if etcd_pod.spec.hostname in running_etcd_members_names]

		for member in etcd_members_not_brought_up:
			if "name" in member:
				member_pod = map_etcd_member_name_to_k8s_pod(member["name"])
				if member_pod:
					print('failed pod start date')
					# Times are in UTC, assuming 24 hour military
					pod_start_date = member_pod.metadata.creation_timestamp.date()
					pod_start_time = member_pod.metadata.creation_timestamp.time()

					pod_grace_start_datetime = member_pod.metadata.creation_timestamp + timedelta(minutes=1)

					print(pod_start_date == datetime.now().date())

					# Means at least 1 day the pod has failed and still there
					if pod_start_date < datetime.now().date():
						print('Pods failed state has been active for more than a day')
						functioning_etcd_pods.append(member_pod)
						member_details = map_etcd_name_to_member_details(member["name"])
						running_etcd_members.append(member_details)
					# This logic only executes for pods that aren't even established in the cluster (no name or clientURL)
					# It won't affect pods that have been established already and happened to fail once, because we are using etcd_members_not_brought_up
					if pod_grace_start_datetime.time() < datetime.now().time():
						print('Pod failed state has been active for more than 1 minutes') 
						functioning_etcd_pods.append(member_pod)
						# Need to handle this error if map returns None
						# Would cause a key error if we shoved a None into this array when we do the lambda mapping below
						member_details = map_etcd_name_to_member_details(member["name"])
						running_etcd_members.append(member_details)

		failed_etcd_pod_names = []

		for etcd_pod in functioning_etcd_pods:
			if etcd_pod.spec.hostname not in local_etcd_monitor:
				local_etcd_monitor[etcd_pod.spec.hostname] = {}
				local_etcd_monitor[etcd_pod.spec.hostname]["ping_count"] = 0
				local_etcd_monitor[etcd_pod.spec.hostname]["unconnectivity_occurences"] = 0
			if etcd_pod.status.pod_ip:
				EtcdApiHandler.set_base_url("http://"+etcd_pod.status.pod_ip+":2379")
				res = EtcdApiHandler.get("/health", timeout=2)
				local_etcd_monitor[etcd_pod.spec.hostname]["ping_count"] += 1

				if ("health" in res) and (res["health"] == 'true'):
					print(etcd_pod.spec.hostname + " health: " + res["health"])
					local_etcd_monitor[etcd_pod.spec.hostname]["health"] = "healthy"
					# The pod has some failed pings, but not enough to trigger migration, and it is currently healthy, then we reset its failed counter
					if local_etcd_monitor[etcd_pod.spec.hostname]["ping_count"] % 15 == 0:
						local_etcd_monitor[etcd_pod.spec.hostname]["unconnectivity_occurences"] = 0
				else:
					local_etcd_monitor[etcd_pod.spec.hostname]["health"] = "unhealthy"
					local_etcd_monitor[etcd_pod.spec.hostname]["unconnectivity_occurences"] += 1
					failed_etcd_pod_names.append(etcd_pod.spec.hostname) # Just the pod's name
					print("cannot reach " + etcd_pod.spec.hostname)

		failed_members = list(filter(lambda mem: mem["name"] in failed_etcd_pod_names, running_etcd_members))

		EtcdApiHandler.set_base_url(etcd_cluster_endpoint)

		print('Detected locally failed etcd pods:', failed_etcd_pod_names)
		print('Failed members still in cluster that need to be removed:', failed_members)

		return failed_members

	try:
		i = 0
		global pv_counter_id, etcd_counter_id

		while True:
			start_time = time.time()
			
			failed_members = check_etcd_pod_responsiveness()
			print('local_etcd_monitor before pop', local_etcd_monitor)

			for failed_member in failed_members:
				name = failed_member["name"]
				if local_etcd_monitor[name]["unconnectivity_occurences"] == 4:

					# local_etcd_monitor.pop(name, None)
					print('local_etcd_monitor after pop', local_etcd_monitor)
					new_etcd_name, new_etcd_name_indexed = get_unique_etcd_name(etcd_counter_id)
					new_pv_name = get_unique_pv_name(pv_counter_id)
					new_domain = get_domain_label()
					new_rack = get_rack_label(new_domain)
					if new_domain and new_rack:
						event_metadata = {
							"OLD_ETCD_MEMBER": failed_member["name"],
							"NEW_DOMAIN": new_domain,
							"NEW_RACK": new_rack,
							"NEW_ETCD_MEMBER_NAME": new_etcd_name,
							"NEW_ETCD_MEMBER_NAME_INDEXED": new_etcd_name_indexed,
							"NEW_PV_NAME": new_pv_name
						}
						queue_migration_events(funcs=[migration_via_removal_add], args=[(failed_member["name"], new_etcd_name, new_pv_name, new_domain, new_rack)], types=['MIGRATION'], events_metadata=[event_metadata])
						pv_counter_id += 1
						etcd_counter_id += 1
						update_cluster()

			else:
				print("All etcd instances are healthy and can be reached")

			i += 1
			print("------")

			time.sleep(interval - ((time.time() - start_time) % interval))

	except KeyboardInterrupt:
		print('exiting script')
		exit


lock = threading.Lock()

# events_metadata is used for determing the domain abstraction to get details on what that event will do the cluster
"""
:param funcs: list of events to execute
:param args: list of tuples of arguments for events
:param types: list of event types for each function
:param events_metadata: list of metadata information for each event
"""
def queue_migration_events(funcs, args, types, events_metadata):
	print('Adding migration event:', types)
	print('Migration details:', events_metadata)
	lock.acquire()
	for func, arg, event_type, event_metadata in zip(funcs, args, types, events_metadata):
		pending_migrations_queue.put([func, arg, event_type, event_metadata])
	lock.release()

def get_queue():
	return pending_migrations_queue

def event_queue_monitoring():
	try:
		while True:
			time.sleep(5)
			start_time = time.time()
			print('CHECKING FOR MIGRATION EVENTS -----------------------------------------------------------')
			print(pending_migrations_queue.qsize())
			if not etcd_pod_being_brought_up() and pending_migrations_queue.qsize() > 0:
				# Get the state first since we need to to check if the migration will go through
				# before taking the lock
				domain_struct = get_domain_structure_abstraction()
				read_get_lock.acquire()
				event = pending_migrations_queue.get()

				print('THIS IS THE EVENT', event)
				func = event[0]
				args = event[1]
				event_type = event[2]
				event_metadata = event[3]

				print('Doing a', event_type)

				if event_type != 'REMOVE':
					rack_name = event_metadata["NEW_RACK"]
					# The func uses the same lock. So this would get stuck. We can try pass a parameter saying "dont use the lock as we are coming from event queue"
					# Better yet, just get the domain struct above, before we take the lock in the event_queue
					# domain_struct = get_domain_structure_abstraction()
					nodes = []

					print(rack_name)

					for idx, domain in enumerate(domain_struct):
						for rack_idx, rack in enumerate(domain["racks"]):
							if rack["name"] == rack_name:
								nodes = rack["node_objects"]
								break

					if (nodes and (len(get_available_nodes_from_nodes(nodes)) > 0)):
						print('Proceeding with the event')
						# Continue with the migration event
						func(*args)
					else:
						# Event will just be popped off from queue and the next time the domain_abstraction is called, it will be show the state where the migration was never made
						print('This migration is no longer valid as the nodes for this domain/rack have become unavailable')

					# Stall until we see the pod coming up and the member in the member list and then release the lock
					# This will prevent the domain_structure_abstraction from missing this during the slight second window when it can be tracked from the pending states -> current states
					# Use the pod itself as an indicator since that's how we store etcd details in our domain abstraction
					new_etcd_name = event_metadata["NEW_ETCD_MEMBER_NAME_INDEXED"]
					new_etcd_pod = map_etcd_member_name_to_k8s_pod(new_etcd_name)
					if new_etcd_pod == None:
						while(new_etcd_pod == None):
							new_etcd_pod = map_etcd_member_name_to_k8s_pod(new_etcd_name)
							time.sleep(2)


				read_get_lock.release()


			time.sleep(10 - ((time.time() - start_time) % 10))
		print('AFTER POPPING THAT EVENT OUT FROM MONITORING')
		# update_cluster()

	except KeyboardInterrupt:
		exit

if __name__ == '__main__':
	# Will point to the kubernetes cluster from ~/.kube/config (kubectl co... (2 KB left)
