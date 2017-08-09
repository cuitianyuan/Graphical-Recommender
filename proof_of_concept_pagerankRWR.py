#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 00:28:51 2017

@author: tcui
"""

import numpy as np


c = .15
Pa = np.array([[0, 0, 0, 1, 0, 1, 0]
            ,[0, 0, 0, 0, 0.5, 0, 0.5]
            ,[0, 0, 0, 0, 0.5, 0, 0.5]
            ,[0.5, 0, 0, 0, 0, 0, 0]
            ,[0, 0.5, 0.5, 0, 0, 0, 0]
            ,[0.5, 0, 0, 0, 0, 0, 0]
            ,[0, 0.5, 0.5, 0, 0, 0, 0]])

Ua = np.array([  0
                ,0
                ,0
                ,0
                ,.5
                ,0
                ,.5]).reshape((7,1))

#RWR restart prob
Qa = np.array([  0
                ,1
                ,0
                ,0
                ,0
                ,0
                ,0]).reshape((7,1))

#Pagerank prob (no restart)
Qa2 = np.array([ 1/7
                ,1/7
                ,1/7
                ,1/7
                ,1/7
                ,1/7
                ,1/7]).reshape((7,1))


msgsum = np.dot(Pa, Ua)


for i in range(10):
    Ua = (1-c)*np.dot(Pa,Ua) + c*Qa
#    Ua = (1-c)*np.dot(Pa,Ua) + c*Qa2
    print(Ua)

