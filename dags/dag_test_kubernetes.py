from airflow.decorators import dag, task
from datetime import datetime

@dag(start_date=datetime(2024, 1, 1), schedule=None, catchup=False)
def my_kubernetes_dag():

    @task.kubernetes(
        image="python:3.11-slim", # Oder dein eigenes Image
        name="k8s-python-task",
        namespace="airflow",
        # Hier kannst du Pakete definieren, falls sie nicht im Image sind
    )
    def my_python_logic():
        # DIESER Code läuft im Kubernetes Pod!
        import pandas as pd
        print("Ich laufe isoliert in K8s")
        return {"status": "success"}

    my_python_logic()

my_kubernetes_dag()