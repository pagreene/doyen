# Deployment to AWS

We will use AWS CloudFormation to deploy the EC2 instance. This will create the necessary resources and
network configuration to run ElasticSearch on AWS. The CloudFormation template is [here](template.yaml).

Once you have the instance running, you can ssh into it and follow the instructions below to set up 
ElasticSearch using docker. This will first require mounting the EBS volume to the EC2 instance and
configuring the docker daemon to use it.

## Deploy the EC2 instance with CloudFormation

To integrate with other systems, this elastic search instance must be deployed to AWS. Using
the AWS CloudFormation template, the necessary resources can be created as follows:

```bash
aws cloudformation create-stack \
  --stack-name doyen-es \
  --template-body file:///full/path/to/doyen/ingestion_pipeline/aws_deployment/template.yaml \
  --parameters ParameterKey=MyHomeIp,ParameterValue=<your ip> \
               ParameterKey=MyKeyPairName,ParameterValue=<your key pair name>
```
Note that you have to give your local IP address. This will (at least initially) restrict access to the EC2
instance to your home network. Similarly, give the name of an existing EC2 key pair that you have access to. This
will set up ssh credentials for the EC2 instance.

You can monitor the progress of the deployment using

```bash
aws cloudformation describe-stacks --stack-name doyen-es --query 'Stacks[0].StackStatus'
```

If there is an error, you can examine the logs using

```bash
aws cloudformation describe-stack-events --stack-name doyen-es
```

You can update the stack using the same command as above, but with `update-stack` instead of `create-stack`:

```bash
aws cloudformation update-stack \
  --stack-name doyen-es \
  --template-body file:///full/path/to/doyen/ingestion_pipeline/aws_deployment/template.yaml \
  --parameters ParameterKey=MyHomeIp,ParameterValue=<your ip>
```

## Setting up Docker

Once the EC2 instance is running, you can ssh into it using the key pair you specified above:

```bash
ssh -i <path to private key> ec2-user@<public ip address>
```

Once you have ssh'd into the EC2 instance, you will need to install docker. You can do this as follows:

```bash
sudo amazon-linux-extras install docker
```

You will need to mount the EBS volume to the EC2 instance. First, you will need to find the volume in
/dev. You can do this using `lsblk`:

```bash
lsblk --output NAME,FSTYPE,SIZE,MOUNTPOINT
```

This should produce something like:

```
NAME          FSTYPE  SIZE MOUNTPOINT
nvme1n1               100G 
nvme0n1                 8G 
├─nvme0n1p1   xfs       8G /
└─nvme0n1p128           1M 
```

Note the much larger volume that is not mounted or formatted. If you see that FSTYPE is xfs and that
there is already a mount point, you're all set and can skip ahead to Configuring Docker.

### Formatting and mounting the EBS volume

You will first need to format it as xfs:

```bash
sudo mkfs -t xfs /dev/nvme1n1
```

Next, you will want to create the directory where you will mount the volume, and then mount it:

```bash
sudo mkdir /volume
sudo mount /dev/nvme1n1 /volume
```

### Configuring Docker to use the EBS volume

First, stop the docker daemon:

```bash
sudo service docker stop
```

Next, you will need to configure the docker daemon to use the EBS volume. To do this, you will need to
create a file at `/etc/docker/daemon.json` with the following contents using your favorite text editor:
```json
{
  "data-root": "/volume/docker"
}
```
You will likely need to use `sudo` to edit the file. Once you have done this, you will need to relocate
any existing docker images and containers to the new location. You can do this as follows, also renaming
the old docker directory as an extra precaution:

```bash
sudo mv /var/lib/docker/* /volume/docker
sudo mv /var/lib/docker /var/lib/docker.bak
```

Finally, you can start the docker daemon:

```bash
sudo service docker start
```

## Setting up ElasticSearch

You are now free to set up ElasticSearch as shown in the [main README](../../README.md), with a single modification:
when you run the docker container, you will need to add two environment variables to the command:

```bash
... --env "discovery.type=single-node" --env "network.host=0.0.0.0" ...
```

This will allow the container to be accessed from outside the EC2 instance. You can now access the ElasticSearch
instance from your local machine using the public IP address of the EC2 instance.

I also suggest using volumes to persist the data in ElasticSearch. This will allow you to stop and start the
container without losing data. You can do this by adding the following to the docker command:

```bash
... --volume /volume/esdata:/usr/share/elasticsearch/data ...
```
