# PeriFlow CLI

<p align="center">
  <img src="./doc/assets/logo.svg" width="30%" alt="system">
</p>

Welcome to PeriFlow ☁︎

PeriFlow is a reliable, speedy, and efficient service for training and serving your own large-scale AI model on any data of your choice. PeriFlow makes use of the cloud infrastructure without your need to invest in on-premise supercomputers. With one click, PeriFlow loads your data and runs optimized massive-scale AI training, handling any headaches that may arise plus visualizing your training progress.

Please visit [docs.periflow.ai](https://docs.periflow.ai) for the detailed guide:

- [CLI documentation](https://docs.periflow.ai/cli/intro)
- [Python SDK documentation](https://docs.periflow.ai/sdk/intro)
- [PeriFlow tutorials](https://docs.periflow.ai/tutorial/intro)

## Installation

You can simply install the package using `pip`.

```sh
pip install periflow-cli
```

## Basic Commands

PeriFlow CLI commands start with the app name prefix `pf`.

```sh
pf [OPTIONS] COMMAND [ARGS]...
```

You can see the detail of each command using one of the following:

```sh
pf COMMAND
pf COMMAND -h
pf COMMAND --help
```

## Workflow

You may go through the following steps when training AI models in PeriFlow.

1. Sign in.
2. Create/upload a dataset.
3. Run a job and monitor the job status.
4. Download trained model checkpoints to your local computer or deploy it!

### Step 1. Sign In

You can login to PeriFlow with the following command.

```sh
pf login
```

### Step 2. Manage Datasets

PeriFlow manages multiple datasets for your jobs. Once you create a dataset, the dataset can be easily used in any job you want. You can create datasets in two different ways:

1. Upload the dataset files from your local file system.
2. Link(register) the dataset files from your cloud storage (e.g., AWS S3, Azure Blob Storage, Google Cloud Storage).

```sh
# Option 1)
pf dataset create [OPTIONS]

# Option 2)
pf dataset upload [OPTIONS]
```

For the second option, you need to create a credential to access your cloud storage with `pf credential create [OPTIONS]` command in advance. You can also list, view, edit and delete the datasets using the CLI. Please use `pf dataset --help` for more details.

### Step 3. Manage Job

You can easily configure jobs using a YAML file or CLI options and run them in cloud virtual machines with as many as GPUs you want. These are the options that you can configure for a job:

- Cloud virtual machine type
- The number of GPUs
- Docker image and command
- Workspace directory
- Dataset
- Model checkpoint
- Slack Notification
- Weights & Biases
- Distributed training

#### Job Type: "Custom" & "Predefined"

There are two types of jobs in PeriFlow: "custom" and "predefined".

- **Custom Job**: Jobs running with your own source codes are categorized as "custom" jobs. You can freely customize a job with your own source code and own docker image.
- **Predefined Job**: PeriFlow provides a predefined job training service for some popular AI models like GPT-3. You can easily configure the predefined jobs without your effort to find hyper-parameters and distributed training configurations. Instead, you can use pre-configured hyper-parameters and train the models in our highly optimized training engine.

#### Job Configuraion File

You can describe the job setting in configuration YAML files. By using `pf job template create [OPTIONS]` command, you can get a job configuration template file generated with the required fields that you need to fill in. For example, suppose that you want to run a custom job with the following requirements:

- Use private docker image.
- Use a training dataset you've created.
- The training will be resumed from a specific model checkpoint file.
- Get Slack notification to get notified of the job status.

To make the configuration file for this kind of job, you may select the following options in the interactive prompt from `pf job template create [OPTIONS]` command.

```txt
$ pf job template create -s /path/to/config.yaml    # Interactive prompt will be started.
What kind of job do you want?
 (predefined, custom)
>> custom
Will you use your private docker image? (You should provide credential). [y/N]
>> y
Do you want to run the job with the scripts in your local directory? [y/N]
>> y
Will you use a dataset for the job? [y/N]
>> y
Will you use an input checkpoint for the job? [y/N]
>> y
Does your job generate model checkpoint files? [y/N]
>> y
Will you run distributed training job? [y/N]
>>
Will you use W&B monitoring for the job? [y/N]
>>
Do you want to get a Slack notification for the job? [y/N]
>> y
Do you want to open an editor to configure the job YAML file? (default editor: nvim) [y/N]
>>
```

Then, the configuration template file will be saved at `/path/to/config.yaml` with the following content.

```yaml
# The name of job
name:

# The name of vm type
vm:

# The number of GPU devices
num_devices:

# Configure your job!
job_setting:
  type: custom

  # Docker config
  docker:
    # Docker image you want to use in the job
    image:
    # Bash shell command to run the job.
    #
    # NOTE: PeriFlow automatically sets the following environment variables for PyTorch DDP.
    #   - MASTER_ADDR: Address of rank 0 node.
    #   - WORLD_SIZE: The total number of GPUs participating in the task.
    #   - RANK: Rank of the current process.
    #   - LOCAL_RANK: Local rank of the current process in the node.
    #   - NODE_RANK: Index of the current node.
    command:
    credential_id:
  # Path to mount your workspace volume. If not specified, '/workspace' will be used by default.
  workspace:
    mount_path:

# Configure dataset
data:
  # The name of dataset
  name:
  # Path to mount your dataset volume
  mount_path:

# Checkpoint config
checkpoint:
  input:
    # UUID of input checkpoint
    id:
    # Input checkpoint mount path
    mount_path:
  # Path to output checkpoint
  output_checkpoint_dir:

# Additional plugin for job monitoring and push notification
plugin:
  slack:
    credential_id:
    channel:
```

When you finished filling in the fields in the configuration file, you can use it to run jobs with `pf job run -f /path/to/config.yaml`.

#### Run & Monitor Jobs

You can run jobs and monitor the status using the CLI.

- `pf job run [OPTIONS]`: Run a job.
- `pf job list [OPTIONS]`: List up jobs.
- `pf job view JOB_ID`: View the detail of a specific job.
- `pf job log [OPTIONS] JOB_ID`: View the console output (i.e., stdout, stderr) of a specific job.

For more detail, please use `-h` or `--help` option.

### Step 4. Manage Model Checkpoints

You can manage model checkpoint files generated from your jobs using the CLI.

> 📝 NOTE: `pf job view JOB_ID` will show a list of checkpoints generated from the job.

- `pf checkpoint list [OPTIONS]`: List up checkpoints.
- `pf checkpoint view CHECKPOINT_ID`: View the detail of a specific checkpoint.
- `pf checkpoint download [OPTIONS] CHECKPOINT_ID`: Download checkpoint files to your local file system.

## Getting Help

- Contact us via contact@friendli.ai to get trial access to PeriFlow.
- If you have any issues or questions, please visit [PeriFlow Discussion Forum](https://discuss.friendli.ai/) and get support.
