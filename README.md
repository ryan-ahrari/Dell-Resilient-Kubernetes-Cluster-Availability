# Dell-Resilient-Kubernetes-Cluster-Availability

<h2> What our project is all about </h2>

In this project we design a protocol to provide data resiliency for Kubernetes. 



<p align="center">
   <img src="images/Kubernetes_logo.png" alt="Kubernetes logo"/>
</p>

<p align="center">
  <img src="images/etcd.png" alt="etcd logo"/>
</p>

This protocol works by migrating failed etcd pods. To do this we communicate with the etcd and Kubernetes API's to get information about the Kubernetes cluster and the etcd cluster. This information is then used to decide which pods are to be migrated and and where they will be migrated to. 

<h2> How to use our project </h2>
First you would need to make sure that you are running a multinode vanilla cluster (meaning no minikube), and be running etcd as an external service.  

<br> </br>

After you have that set up, you could deploy our protocol by running the following script:

```
   python3 resilient.py
```
