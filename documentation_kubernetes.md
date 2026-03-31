# install airflow

## start cluster
<code>minikube start --driver=docker --memory=8192 --cpus=4</code>

## install airflow
<code>helm repo add apache-airflow https://airflow.apache.org</code>

<code>helm repo update</code>

<code>kubectl create namespace airflow</code>

### git_sync

#### create ssh keys

<code>ssh-keygen -t ed25519 -C "airflow-git-sync" -f ./airflow_key -N ""</code>

#### kubernetes secret for git_sync

<code>kubectl create secret generic airflow-git-ssh-secret \
  --from-file=gitSshKey=./airflow_key \
  -n airflow</code>

### update airflow with helm

<code>helm upgrade --install airflow apache-airflow/airflow -f values.yaml --namespace airflow --set logs.persistence.enabled=true</code>

<code>kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow</code>
<code>kubectl port-forward --address 0.0.0.0 svc/airflow-api-server 8080:8080 -n airflow</code>



