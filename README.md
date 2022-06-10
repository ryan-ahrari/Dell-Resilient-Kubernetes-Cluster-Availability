# Dell-Resilient-Kubernetes-Cluster-Availability

<h2> What our project is all about </h2>

In this project we design a protocol to provide data resiliency for Kubernetes. 



<p align="center">
   <img src="images/Kubernetes_logo.png" alt="Kubernetes logo" width=50%/>
</p>

<p align="center">
  <img src="images/etcd.png" alt="etcd logo"/>
</p>

This protocol works by migrating unhealthy or failed etcd pods. To do this we communicate with the etcd and Kubernetes API's to get information about the Kubernetes cluster and the etcd cluster. This information is then used to decide which pods are to be migrated and and where they will be migrated to. 

<h2> How to use our project </h2>

<h3> First you need some setup </h3>
First you would need to make sure that you are running a multinode vanilla cluster (meaning no minikube), which can be setup on a single computer using virtual machines if you follow this video: <br> https://www.youtube.com/watch?v=s3EdEILiLdI. </br> You would need at least 1 master and 4 worker nodes. 

<br> </br>

Then you would want to run etcd as an external service. To do this you would first need to make sure that all worker nodes that may have an etcd pod has a data directory to mount the pod onto. To make this directory, run the following commands on each worker node:

```
   > sudo su
   > {ENTER PASSWORD}
   > cd /root
   > mkdir data
   > mkdir data/myss
   > ls /data/myss
```
<br> </br>

Once you have the data directory, you can apply the persistent volume files that will be used to bind the pod to a particular worker nodes data directory, and then deploy the actual etcd cluster by doing:

```
   > kubectl apply -f {YAML FILE}
```
on the master node with the following files  in order: 
- pv1.yaml 
- pv2.yaml
- pv3.yaml
- StatefulSet.yaml

You should now have a working Kubernetes cluster that is running etcd as an external service!

<br> </br>
<h3> Now you can run our protocol </h3>

After you have that set up, you could deploy our protocol by running the following script:

```
   > python3 resilient.py
```

<h2> If you run into issues </h2>

Here are some useful commands you can run if you find problems.

<br> </br>

If you see that the etcd cluster is unable to successfully get all pods into a running state, you can run the following commands to delete the etcd cluster and its persistent volumes. 
```
   > kubectl delete -f StatefulSet.yaml
   > kubectl delete pvc --all
   > kubectl delete pv --all
```

<br> </br>

If you are seeing an issue with Kubernetes, there may be several things you can do in order to resolve this. 

First we would suggest to enable sudo mode by doing:
```
   > sudo su
   > {ENTER PASSWORD}
```
Once you are the superuser, resume attempting whatever you were trying before that failed to see if it is now working. 


If its not working still, the next thing we would suggest is to reset the node that is giving you problems, by doing:
```
   > kubeadm reset
```
And then whther it is a master or worker node rejoin or reinitialize the cluster by doing the resepctive command from the above YouTube video.


By this point the majority of issues should be resolved. 
