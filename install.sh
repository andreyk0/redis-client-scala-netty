#!/bin/bash
mvn deploy -DskipTests=true -Durl=sftp://192.168.2.101:9142/repository -DrepositoryId=ftp-repo-dev -DaltDeploymentRepository=ftp-repo-dev::default::sftp://192.168.2.101:9142/repository/
