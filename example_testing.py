import mlflow
from mlflow.tracking import MlflowClient
import requests 
import json
import uuid

base_url = 'https://eurocontrol-dev.collibra.com/rest/2.0/'
AUTH = ('Admin', 'wxcvB3n')

client = MlflowClient()
client.delete_experiment('0')

experiments = mlflow.search_experiments()
for experiment in experiments:
    experiment_id = experiment.experiment_id
    experiment_name = experiment.name
    post_experiment = {
        "name": experiment_name,
        "displayName": experiment_name,
        "domainId":"a58ce69e-5378-4dec-a932-63779035dc9d",
        "typeId": "fc0f0b2a-784e-42d9-97cb-cb7ed6bebc60"
    }
    experiment_post_request = requests.post(url=base_url+'assets', json=post_experiment, auth=AUTH)
    collibra_experiment_call = requests.get(url=base_url+'assets', params={
        "domainId":"a58ce69e-5378-4dec-a932-63779035dc9d", "typeId":"fc0f0b2a-784e-42d9-97cb-cb7ed6bebc60"}, auth=AUTH)
    collibra_experiment_call_content = json.loads(collibra_experiment_call.content)
    collibra_experiment_id = collibra_experiment_call_content['results'][0]['id']
    collibra_experiment_ids = []
    collibra_experiment_ids.append(collibra_experiment_id)
    # Getting run info from mlflow
    run_info = client.list_run_infos(experiment_id)
    runs = mlflow.search_runs(experiment_ids=[experiment_id])
    run_ids = [run.run_id for run in run_info]
    collibra_run_ids = list(map(lambda id: str(uuid.UUID(id)), run_ids))
    run_names = (list(runs['tags.mlflow.runName']))

    # SYNCING MLFLOW RUNS PER EXPERIMENT WITH COLLIBRA
    runs_to_post=[]
    for index in range(len(run_names)):
        runs_to_post.append({
            "name": run_names[index],
            "displayName": run_names[index],
            "domainId": "a58ce69e-5378-4dec-a932-63779035dc9d",
            "typeId": "7a60667d-fec3-4f5c-a091-439d67e5d17f",
            "id": collibra_run_ids[index]
        })
    post_run_answer = requests.post(url=base_url+"assets/bulk", json=runs_to_post, auth=AUTH)

    # IMPORTING THE PARAMETERS AS ATTRIBUTES TO THE ASSET RUN

    ALL_PARAM = [client.get_run(run_id).data.params["depth"] for run_id in run_ids]
    run_param = dict(zip(collibra_run_ids, ALL_PARAM))
    params_to_post=[]
    for run,param in run_param.items():
        params_to_post.append(
    {
        "assetId": run,
        "typeId": "c768d276-fceb-4b1c-a373-2842d3018f29",
        "value": param
    }
        )
    params_post_request = requests.post(url=base_url+'attributes/bulk', auth=AUTH, json=params_to_post)

    # IMPORTING THE METRICS AS ATTRIBUTES TO THE ASSET RUN

    ALL_METRIC = [client.get_run(run_id).data.metrics["accuracy"] for run_id in run_ids]
    for index in range(len(ALL_METRIC)):
        str(ALL_METRIC[index])
    run_metric = dict(zip(collibra_run_ids, ALL_METRIC))

    metrics_to_post=[]
    for run,metric in run_metric.items():
        metrics_to_post.append(
        {
            "assetId": run,
            "typeId": "a853c587-bfc7-4272-b674-26ee40e598a9",
            "value": metric
        }
            )
    metrics_post_request = requests.post(url=base_url+'attributes/bulk', auth=AUTH, json=metrics_to_post)



    # ADDING RELATIONS BETWEEN EXPERIMENTS AND RUNS

    relations_to_post = []

    for experiment in collibra_experiment_ids:
        for run in collibra_run_ids:
            relations_to_post.append({
            "sourceId": f"{experiment}",
            "targetId": f"{run}",
            "typeId": "d76c5104-0280-447d-a210-90b65902ec96"
        })
    post_relations = requests.post(url=base_url+'relations/bulk', auth=AUTH, json=relations_to_post)
    print(post_relations.status_code)








