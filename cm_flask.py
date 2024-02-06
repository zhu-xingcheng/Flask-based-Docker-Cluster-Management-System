"""
# A Simple Claster
- Create cluster ofmachines
- The machines are docker containers
- Cluster manager is a Python program
- The state of the cluster is written to a file
## Assignment #1: Create Cluster Manager
- Cluster manager (CM) is going to be a Python program
- You run the CM and it should be able to create the cluster of containers - let's say 8 containers are created by the CM
- You can think of the cluster as a simple cloud with 8 fake VMs .
- In this assignment you need to support the following commands:create cluster,list cluster,run a simple command in the cluster,stop the cluster,delete all the containers in the cluster

### Assignment #1: More Information
- Cluster create - you give the number of containers to start, the CM would create them - each container is a docker machine
- You can use docker tools - there is a Python interface to all the docker tools - so you can control docker containers using Python programs
- List the running containers - again just use the Python docker tools. To list, you ask the CM to list the containers and it does it for you
- Same way for other commands

## Assignment #2: Data Processing in CM
- Container manager (CM) needs to support data volumes
- Data volume is a storage for data - the data you put there is available for all the containers
- Put a large array of data and it is available for all containers
- Each container can process a portion of the array
- They can print the local results to screen

### Assignment #2: Data Processing in CM
- Let's have array of 100,000 numbers
- We create a cluster with 4 containers
- Each container process 1/4 of the array - available through the data volume
- Each container prints a result for its portion - we ask each container to print the sum, average, max, min, standard deviation for their portions of the array
- So, we get four results for each parameter

## Assignment #3: AI Tasks in CM
- Cluster manager (CM) is dealing with Docker containers
- We can load Tensorflow into Docker containers quite easily
- Lets write a program to do linear regression on an example data set using Tensorflow
"""

import datetime

import docker
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)


class ClusterManager:
    def __init__(self):
        # init Docker client
        self.docker_client = docker.from_env()
        self.cluster_containers = self.docker_client.containers.list()

    def list_images(self):
        # list all images
        images = self.docker_client.images.list()
        res = list()
        if images:
            print("Available Images:")
            for image in images:
                print(f" - {image.tags[0]}")
                res.append(str(image.tags[0]))
        else:
            print("No images available.")
        return res

    def list_containers(self):
        # list all containers
        containers = self.docker_client.containers.list()
        res = list()
        if containers:
            print("Running Containers:")
            self.cluster_containers = []
            for container in containers:
                print(f" - {container.name}")
                self.cluster_containers.append(container)
                res.append(f"{container.name}:{str(container)}")
        else:
            print("No running containers.")
        return res

    def run_container(self, image_name, container_name):
        # run a container
        try:
            container = self.docker_client.containers.run(
                image_name,
                detach=True,
                name=container_name,
                stdin_open=True,
                tty=True,
            )
            print(f"Container '{container_name}' is now running.")
            self.cluster_containers.append(container)
            return container.name
        except docker.errors.ImageNotFound:
            print(f"Error: Image '{image_name}' not found. Please pull the image first.")
        except docker.errors.APIError as e:
            print(f"Error: {e}")
        return ""

    def stop_container(self, container_name):
        # stop the container
        try:
            container = self.docker_client.containers.get(container_name)
            container.stop()
            print(f"Container '{container_name}' has been stopped.")
            self.cluster_containers.remove(container)
            return container.name
        except docker.errors.NotFound:
            print(f"Error: Container '{container_name}' not found.")
        return ""

    def stop_cluster(self):
        # stop the cluster
        res = []
        if self.cluster_containers:
            print("Stopping all containers in the cluster:")
            res = [container.name for container in self.cluster_containers]
            for container in self.cluster_containers:
                container.stop()
                print(f" - {container.name}")
            self.cluster_containers = []
            print("All containers in the cluster have been stopped.")
        else:
            print("No containers in the cluster.")
        return res

    def run_command_in_cluster(self, container_name, command):
        # run a command in the cluster
        try:
            container = self.docker_client.containers.get(container_name)
            result = container.exec_run(command)
            print(f"Command executed in container '{container_name}':")
            print(result.output.decode())
            return str(result.output.decode()).replace("\n", " ")
        except docker.errors.NotFound:
            print(f"Error: Container '{container_name}' not found.")
            return ""
        except docker.errors.APIError as e:
            print(f"Error: {e}")
            return ""

    def remove_container(self, container_name):
        # remove a container
        try:
            container = self.docker_client.containers.get(container_name)
            container.remove()
            print(f"Container '{container_name}' has been removed.")
            self.cluster_containers.remove(container)
            return container_name
        except docker.errors.NotFound:
            print(f"Error: Container '{container_name}' not found.")
            return ""

    def remove_cluster(self):
        # remove the cluster
        all_containers = self.docker_client.containers.list(all=True)
        res = []
        if all_containers:
            print("Removing all containers in the cluster:")
            for container in all_containers:
                res.append(f"{container.name}:{container}:{container.status}")
                print(f" - {container.name}")
                container.remove(force=True)
            self.cluster_containers = []
            print("All containers in the cluster have been removed.")
        else:
            print("No containers in the cluster.")
        return res

    def save_to_log_file(self, log):
        current_datetime = datetime.datetime.now().strftime("[%Y-%m-%d %H:%M:%S] ")
        log_with_datetime = current_datetime + log.replace("\n", "") + "\n"
        with open("log.txt", "a") as log_file:
            log_file.write(log_with_datetime)

    def read_log_file(self):
        try:
            with open("log.txt", "r") as log_file:
                return log_file.read()
        except FileNotFoundError:
            with open("log.txt", "w") as log_file:
                log_file.write("")
            with open("log.txt", "r") as log_file:
                return log_file.read()

    def calculate_in_container(self, container_name):
        try:
            container = self.docker_client.containers.get(container_name)
            response = container.exec_run(
                cmd="python -c 'import numpy as np;data = np.random.randint(1,100,25000); print(f\"sum:{np.sum(data)} mean:{np.mean(data)} max:{np.max(data)} min:{np.min(data)} std:{np.std(data)}\");'")
            result = response.output.decode().strip()

            print(f"Sum calculated in container '{container_name}': {result}")
            return result
        except docker.errors.NotFound:
            print(f"Error: Container '{container_name}' not found.")
            return ""
        except docker.errors.APIError as e:
            print(f"Error: {e}")
            return ""

    def assignment3(self):

        container = self.docker_client.containers.run(image='tensorflow', name='assignment3',
                                                      detach=True, stdin_open=True, tty=True)
        response = container.exec_run(cmd="python Ireg.py")
        print("Container:", container.name)
        exit_code = response.exit_code
        print("Exit code:", exit_code)
        output = response.output.decode()
        print("Output:\n", output)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/list_images')
def list_images():
    images = manager.list_images()
    if len(images) > 0:
        manager.save_to_log_file(str({'All Available Images': images}))
        return jsonify({'All Available Images': images})
    else:
        manager.save_to_log_file({'No Images Available': "null"})
        return jsonify('No Images Available')


@app.route('/list_containers')
def list_containers():
    containers = manager.list_containers()
    if len(containers) > 0:
        manager.save_to_log_file(str({'All Running Containers': containers}))
        return jsonify({'All Running Containers': containers})
    else:
        manager.save_to_log_file('No Running Containers')
        return jsonify('No Running Containers')


@app.route('/run_container', methods=['POST'])
def run_container():
    data = request.get_json()
    image_name = data.get('image_name')
    container_name = data.get('container_name')
    result = manager.run_container(image_name, container_name)
    if result:
        manager.save_to_log_file(f"Container {container_name} Started")
        return jsonify(f"Container {container_name} Started")
    else:
        manager.save_to_log_file("Container Running Cancelled")
        return jsonify(f"Container Running Cancelled")


@app.route('/stop_container', methods=['POST'])
def stop_container():
    data = request.get_json()
    container_name = data.get('container_name')
    result = manager.stop_container(container_name)
    if result is not None:
        manager.save_to_log_file(f"Container {container_name} stopped")
        return jsonify(f"Container {container_name} stopped")
    else:
        manager.save_to_log_file("Container Stopping Cancelled")
        return jsonify(f"Container Stopping Cancelled")


@app.route('/stop_cluster', methods=['POST'])
def stop_cluster():
    result = manager.stop_cluster()
    if len(result) > 0:
        manager.save_to_log_file(str({'All Running Clusters': result}))
        return jsonify({'All Containers Stopped in Cluster': result})
    else:
        manager.save_to_log_file('No Containers in the Cluster')
        return jsonify('No Containers in the Cluster')


@app.route('/run_command_in_cluster', methods=['POST'])
def run_command_in_cluster():
    data = request.get_json()
    container_name = data.get('container_name')
    command = data.get('command')
    result = manager.run_command_in_cluster(container_name, command)
    if result:
        manager.save_to_log_file(str({'Command Executed in Container': result}))
        return jsonify({'Command Executed in Container': result})
    else:
        manager.save_to_log_file('Command Execution Cancelled')
        return jsonify('Command Execution Cancelled')


@app.route('/remove_container', methods=['POST'])
def remove_container():
    data = request.get_json()
    container_name = data.get('container_name')
    result = manager.remove_container(container_name)
    if result:
        manager.save_to_log_file(str({'Container Removed': container_name}))
        return jsonify({'Container Removed': container_name})
    else:
        manager.save_to_log_file('Container Removal Cancelled')
        return jsonify('Container Removal Cancelled')


@app.route('/remove_cluster', methods=['POST'])
def remove_cluster():
    result = manager.remove_cluster()
    if len(result) > 0:
        manager.save_to_log_file(str({'All Containers Removed in Cluster': result}))
        return jsonify({'All Containers Removed in Cluster': result})
    else:
        manager.save_to_log_file('No Containers in the Cluster')
        return jsonify('No Containers in the Cluster')


@app.route('/get_log_file')
def get_log_file():
    log_content = manager.read_log_file()
    return jsonify({'log_content': log_content})


@app.route('/run_ai_task', methods=['POST'])
def run_ai_task():
    image_name = 'tensorflow:my'
    container_name = 'ai_task'
    container_name = manager.run_container(image_name, container_name)
    container = manager.docker_client.containers.get(container_name)
    response = container.exec_run("python /app/lreg.py")
    result = response.output.decode().strip()
    data, stat = manager.docker_client.api.get_archive(container_name, "/app/fig.png")
    with open("./fig.png", 'wb') as f:
        for chunk in data:
            f.write(chunk)
    return jsonify(result)


@app.route('/run_parallel_computation', methods=['POST'])
def run_parallel_computation():
    container_names = []
    parallel_n = 4
    for i in range(0, parallel_n):
        image_name = 'python:3.8'
        container_name = f'container_partition_{i}'
        container_name = manager.run_container(image_name, container_name)
        container_names.append(container_name)

    if container_names:
        manager.save_to_log_file(f"Parallel computation started in containers: {container_names}")

        # Calculate in each container
        sum_results = {}
        for container_name in container_names:
            sum_result = manager.calculate_in_container(container_name)
            sum_results[container_name] = sum_result
        print(sum_results)
        return jsonify({'Containers': container_names, 'Sum Results': sum_results})
    else:
        manager.save_to_log_file("Parallel computation cancelled")
        return jsonify("Parallel computation cancelled")


if __name__ == '__main__':
    manager = ClusterManager()
    app.run(debug=True)
