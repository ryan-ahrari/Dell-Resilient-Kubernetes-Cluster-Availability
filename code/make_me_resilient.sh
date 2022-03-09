#!/usr/bin/env bash

#----------------------------------------------------------------------------------
# Infinte for loop, only will break in DR (Disaster Recovery) scenario

    #----------------------------------------------------------------------------------
    # inital etcd health check script
        # if no response from node, get error message
            # write, or append error message to etcd_failure_error.txt
            # based on error message, call appropriate error function          
    #----------------------------------------------------------------------------------
    
    #----------------------------------------------------------------------------------
    # error n (if n < [1/2 cluster size]) nodes down 
        # find number of members in cluster
            # if n<[members in cluster]
                # place instances of etcd on cluster members
            # if n>[members in cluster]
                # join 2*[n-members in cluster] new members to cluster
                    # place [n-members in cluster] instances of etcd on existing cluster members
                    # place remaining instances in new members        
    #----------------------------------------------------------------------------------
    
    #----------------------------------------------------------------------------------
    # error more than 1/2 cluster nodes down
        # DR scenario
            # every 1 sec check if DR has completed 
                # when DR done do next health check
    #----------------------------------------------------------------------------------
    
    #----------------------------------------------------------------------------------
    # confirmation of health check script
        # if all nodes now healthy
            # wait 5 seconds before going through script again
        # else immediately rerun script
    #----------------------------------------------------------------------------------
    
#----------------------------------------------------------------------------------
