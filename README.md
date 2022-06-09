# Dell-Resilient-Kubernetes-Cluster-Availability
<h1> What our project is all about </h1>

In this project we design a protocol to provide data resiliency for Kubernetes. 



<p align="center">
   <img src="images/Kubernetes_logo.png" alt="Kubernetes logo"/>
</p>

<p align="center">
  <img src="images/etcd.png" alt="etcd logo"/>
</p>

This protocol works by migrating failed etcd pods. To do this we communicate with the etcd and Kubernetes API's to get information about the Kubernetes cluster and the etcd cluster. This information is then used to decide which pods are to be migrated and and where they will be migrated to. 

<h1> How to use our project </h1>
First you would need to make sure that you are running a multinode vanilla cluster (no minikube!), and be running etcd as an external service. 

After you have that set up, you could run our script protocol with the following command
```
  python3 resillient.py
```
