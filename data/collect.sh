#!/bin/bash

kaggle datasets download -d ntnu-testimon/paysim1
unzip paysim1.zip

aws s3 cp paysim1.zip s3://$1