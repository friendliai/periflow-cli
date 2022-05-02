# 이미지 분류 모델 학습하기

본 예시에서는 이미지 분류 모델 학습을 위한 간단한 PyTorch 코드에 PeriFlow SDK를 적용해보고, PeriFlow CLI를 사용하여 클라우드 머신에서 학습을 수행하는 방법을 설명합니다. 예시에서 사용된 Configuration YAML 파일과 Python 코드는 [SDK 예제 레포](https://github.com/friendliai/periflow-python-sdk/tree/main/examples/cifar)에서 확인 가능합니다.

## Requirements

- `pip install periflow-cli`로 CLI 패키지를 설치합니다.
- `pip install periflow_sdk`로 SDK 패키지를 설치합니다.
- [공통 가이드](./common_step.md)에 설명된 과정들이 완료되어야 합니다.
- 본 튜토리얼에서는 CIFAR-100 데이터셋을 사용합니다. [다운로드 링크](https://www.cs.toronto.edu/~kriz/cifar-100-python.tar.gz)를 클릭하여 파일을 다운받고 압축을 해제한 뒤 [Dataset 생성](./common_step.md#dataset-생성) 매뉴얼을 따라 Datastore에 데이터셋을 생성 합니다. 데이터셋 생성이 잘 이루어 졌다면 `pf datastore view` 커맨드를 사용했을 때 다음과 같은 결과가 보여야 합니다.

```sh
$ pf datastore view my-cifar-100  
╭─────────────────────────── Overview ───────────────────────────╮
│  Name          my-cifar-100                                    │
│  Cloud         fai                                             │
│  Region        -                                               │
│  Storage Name  -                                               │
│  Active        Y                                               │
╰────────────────────────────────────────────────────────────────╯
╭──────────────────────────── Files ─────────────────────────────╮
│ /                                                              │
│ └── 📂 cifar-100-python                                        │
│     ├── test (31.0 MB)                                         │
│     ├── meta (1.5 kB)                                          │
│     └── train (155.2 MB)                                       │
╰────────────────────────────────────────────────────────────────╯
╭─────────────────────────── Metadata ───────────────────────────╮
│ {}                                                             │
╰────────────────────────────────────────────────────────────────╯
```

## SDK 적용

[`main.py`](https://github.com/friendliai/periflow-python-sdk/blob/main/examples/cifar/main.py)에는 간단한 PyTorch 학습 코드에 PeriFlow SDK가 적용되어 있습니다. 기존 PyTorch 코드에 추가 되어야 할 부분은 다음 세 줄입니다.

1. `pf.init(total_train_step=total_steps)`
2. `with pf.train_step():`
3. `pf.upload_checkpoint()`

### `pf.init`

PeriFlow를 initialize 하는 부분입니다. 이 함수는 다른 PeriFlow SDK 함수들 보다 반드시 먼저 호출되어야 합니다. Argument로는 전체 학습 스텝 수를 넣습니다.

### `pf.train_step`

`pf.start_step()`과 `pf.end_step()`을 포함하는 컨텍스트 매니저입니다. `pf.train_step()`을 사용하는 대신 `pf.start_step()`과 `pf.end_step()`을 사용해도 무방합니다. `pf.start_step()`은 매 학습 iteration이 시작될 때 호출 되어야 하며, `pf.end_step()`은 매 학습 iteration이 끝날 때 호출 되어야 합니다.

### `pf.upload_checkpoint`

`torch.save()`로 저장된 체크포인트를 업로드 합니다. `pf.upload_checkpoint()`로 업로드 된 체크포인트는 PeriFlow CLI에서 `pf checkpoint view` 또는 `pf checkpoint list`로 확인이 가능합니다.

위의 3가지 함수들을 모두 적용하면 아래와 같은 학습 스크립트가 완성 됩니다. 코멘트가 달린 부분이 기존 PyTorch 코드에 추가된 PeriFlow SDK에 해당합니다.

```python
# @main.py

    pf.init(total_train_steps=total_steps)  # 다른 SDK가 호출되기 전에 init.

    train_iterator = iter(train_loader)
    net.train()
    while step < total_steps:
        try:
            inputs, labels = next(train_iterator)
            inputs = inputs.to(net.device)
            labels = labels.to(net.device)
        except StopIteration:
            optimizer.zero_grad()
            epoch += 1
            train_iterator = iter(train_loader)
            continue

        with pf.train_step():   # 하나의 training iteration을 감싸 줍니다.
            loss, learning_rate = train_step(
                inputs=inputs,
                labels=labels,
                model=net,
                loss_function=loss_function,
                optimizer=optimizer,
                lr_scheduler=lr_scheduler
            )
        step += 1

        if args.save and step % args.save_interval == 0:
            torch.save(
                {
                    "latest_step": step,
                    "model": net.state_dict(),
                    "optimizer": optimizer.state_dict(),
                    "lr_scheduler": lr_scheduler.state_dict()
                },
                os.path.join(args.save, "checkpoint.pt")
            )

            pf.upload_checkpoint()  # 현재 step(i.e., save step)에서 모델 체크포인트를 업로드 합니다.
```

이렇게 SDK가 적용된 `main.py` 파일을 로컬에 저장합니다.

## Configuration YAML 파일

Datastore에 CIFAR-100 데이터셋(`my-cifar-100`)을 업로드 했고, 로컬에 SDK가 적용된 코드(`main.py`)가 준비되었다면, 마지막으로 YAML 파일로 Job의 세부사항을 명시하는 과정만이 남았습니다. [`pf-template.yml`](https://github.com/friendliai/periflow-python-sdk/blob/main/examples/cifar/pf-template.yml)에는 `main.py`를 수행하기 위한 configuration 예시가 나와 있습니다.

```yaml
# pf-template.yml

# The name of experiment
experiment: computer-vision

# The name of job
name: cifar-job

# The name of vm type
vm: azure-16gb-v100-4g-eastus-spot

# The number of GPU devices
num_devices: 8

# Configure your job!
job_setting:
  type: custom

  # Docker config
  docker:
    # Docker image you want to use in the job
    image: friendliai/periflow:sdk
    # Bash shell command to run the job
    # NOTE: PeriFlow automatically sets the following environment variables for PyTorch DDP.
    #   - MASTER_ADDR: Address of rank 0 node.
    #   - WORLD_SIZE: The total number of GPUs participating in the task.
    #   - NODE_RANK: Index of the current node.
    #   - NPROC_PER_NODE: The number of processes in the current node.
    command: >
      cd /workspace/cifar && torchrun --nnodes $NUM_NODES --node_rank $NODE_RANK --master_addr $MASTER_ADDR --master_port 6000 --nproc_per_node 4 main.py \
        --model resnet50 \
        --dataset cifar100 \
        --batch-size 256 \
        --log-interval 100 \
        --total-epochs 50 \
        --save-interval 100 \
        --test-interval 100 \
        --num-dataloader-workers 4 \
        --data-path /workspace/data \
        --save /workspace/ckpt \
        --load /workspace/ckpt
  # Path to mount your workspace volume
  workspace:
    mount_path: /workspace

# Checkpoint config
checkpoint:
  # Path to output checkpoint
  output_checkpoint_dir: /workspace/ckpt

# Configure dataset
data:
  name: my-cifar-100
  mount_path: /workspace/data
```

각 필드가 무엇을 의미 하는지 살펴보도록 하겠습니다.

- `experiment`: 내가 돌릴 Job의 Experiment(i.e., 여러 Job의 묶음, Job의 tag)를 설정합니다. Experiment는 `pf experiment create` 명령어를 통해 생성이 가능하며, 만약 존재하지 않는 Experiment를 사용하려고 하면 `pf job run` 명령어 실행시에 새로운 Experiment를 생성할 수도 있습니다.
- `name`: **[Optional]** Job의 이름입니다. Job의 이름은 중복이 가능합니다.
- `vm`: Job에서 사용할 클라우드 머신의 유형을 입력합니다. 사용 가능한 머신의 유형은 `pf vm list` 명령어를 통해 확인이 가능합니다.

```sh
# Quota는 사용 가능한 머신의 개수를 나타냅니다.
# VM의 이름은 [cloud]-[memory size]-[gpu type]-[gpu count]-[region]-[spot] 형식을 가집니다.

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┓
┃ VM                             ┃ Cloud ┃ Region    ┃ Device ┃ Quota ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━┩
│ azure-32gb-v100-8g-eastus      │ azure │ eastus    │ V100   │ 256   │
│ aws-16gb-v100-8g-us-east-2     │ aws   │ us-east-2 │ V100   │ 256   │
│ azure-40gb-a100-8g-westus2     │ azure │ westus2   │ A100   │ 256   │
│ aws-16gb-v100-8g-us-west-2     │ aws   │ us-west-2 │ V100   │ 64    │
│ azure-32gb-v100-8g-eastus-spot │ azure │ eastus    │ V100   │ 128   │
│ azure-16gb-v100-2g-eastus-spot │ azure │ eastus    │ V100   │ 256   │
│ azure-16gb-v100-1g-eastus-spot │ azure │ eastus    │ V100   │ 64    │
│ azure-16gb-v100-4g-eastus-spot │ azure │ eastus    │ V100   │ 256   │
└────────────────────────────────┴───────┴───────────┴────────┴───────┘
```

- `num_devices`: Job에서 사용할 VM의 개수를 나타냅니다. `vm` 필드에 입력한 머신 유형이 가지고 있는 GPU 개수를 고려하여 입력하는 것을 권장합니다. 위의 YAML 파일에서는 V100 GPU 4개가 장착된 `azure-16gb-v100-4g-eastus-spot` VM을 사용하고 있고 `num_devices`는 8로 설정 하였습니다. 이 경우 `azure-16gb-v100-4g-eastus-spot` 타입의 머신 2개(4x2=8)가 할당 되어 Job이 수행됩니다.
- `job_setting`
  - `type`: "custom"을 입력합니다.
  - `docker`
    - `image`: Job에서 사용할 도커 이미지 이름을 입력합니다. 직접 빌드한 도커 이미지를 사용한다면 도커 이미지에 `periflow_sdk`를 설치해두는 것을 권장합니다. 직접 이미지를 빌드하여 사용하는 대신 위의 예시에 입력된 `friendliai/periflow:sdk` 이미지를 사용할 수도 있습니다. `friendliai/periflow:sdk`에는 PyTorch와 PeriFlow SDK가 설치되어 있습니다.
    - `command`: 학습 프로세스를 실행하기 위한 커맨드를 입력합니다. PyTorch DDP를 사용하는 등의 분산학습 상황을 지원하기 위해 PeriFlow에서는 다음과 같은 분산학습 관련 환경 변수를 자동으로 설정합니다.
      - MASTER_ADDR
      - WORLD_SIZE
      - RANK
      - LOCAL_RANK
      - NODE_RANK
  - `workspace`
    - `mount_path`:  현재 나의 로컬에 있는 `main.py` 파일을 볼륨 마운트 할 곳을 지정합니다. 뒤의 [Job 실행](#job-실행) 섹션에서 자세한 내용이 설명되겠지만 `pf job run`의 `-d` 옵션에 입력한 로컬 디렉토리가 `mount_path` 필드로 마운트 됩니다. 로컬에서 `main.py`의 위치가 `./cifar/main.py`이고 `pf job run ... -d ./cifar` 명령어로 Job을 실행했다면 Job의 실행 환경에서 `main.py`의 위치는 `/workspace/cifar/main.py`가 됩니다.
- `checkpoint`
  - `output_checkpoint_dir`: SDK에서 `pf.upload_checkpoint()`를 호출하였을 때 이 필드에 입력한 경로에 있는 모든 파일들이 업로드 됩니다.
- `data`
  - `name`: [Dataset 생성](./common_step.md#dataset-생성) 매뉴얼을 따라 생성된 CIFAR-100 데이터셋의 이름을 입력합니다. 여기에서는 앞에서 생성한 데이터셋인 `my-cifar-100`을 입력합니다. Datastore에 있는 데이터셋 목록을 확인하려면 `pf datastore list` 명령어를 사용합니다.
  - `mount_path`: 데이터셋을 볼륨 마운트 할 곳을 지정합니다. 여기에선 `/workspace/data`로 경로를 지정하였기 때문에 앞서 생성한 `my-cifar-100` 데이터셋이 `/workspace/data`에 마운트 되어 `/workspace/data/cifar-100-python/train`, `/workspace/data/cifar-100-python/test`, `/workspace/data/cifar-100-python/meta`와 같은 파일 시스템 구조에서 Job이 실행 됩니다.

## Job 실행

이제 모든 준비가 완료되었습니다. 우리는 앞에서 `my-cifar-100` 데이터셋을 생성하였고, 로컬에는 PeriFlow SDK가 적용된 `main.py`와 configuration YAML 파일 `pf-template.yml`이 준비 되었습니다.

```sh
# 현재 로컬의 디렉토리 구조
$ tree
.
├── pf-template.yml
└── cifar
    └── main.py
```

이제 다음 커맨드로 Job을 실행합니다.

```sh
pf job run -f pf-template.yml -d ./cifar
```

- `-f`: Configuration YAML 파일의 경로입니다. 앞에서 작성한 `pf-template.yml`을 입력합니다.
- `-d`: 로컬에 있는 Workspace 디렉토리 경로입니다. 현재 로컬에 SDK가 적용된 `main.py`는 `.cifar/main.py`에 있습니다. `-d` 옵션에 `./cifar`를 입력하면 `./cifar` 디렉토리 전체가 `pf-template.yml` 파일의 `job_setting:workspace:mount_path` 필드에 지정된 경로로 마운트 됩니다. 현재 `pf-template.yml`에는 해당 경로가 `/workspace`로 되어 있기 때문에 `/workspace/cifar/main.py`와 같은 파일 구조에서 Job이 실행 됩니다.

결론적으로 Job이 실행되는 환경의 파일 시스템 구조는 다음과 같습니다.

```sh
/
└── 📂 workspace
    ├─── 📂 cifar
    │    └── main.py
    ├─── 📂 data
    │    └── 📂 cifar-100-python
    │        ├── test
    │        ├── meta
    │        └── train
    └─── 📂 ckpt
```

`pf-template.yml`의 `job_setting:docker:command` 필드에 입력된 shell 명령어는 이러한 파일 시스템 구조를 고려하여 작성되어 있습니다.

```sh
    command: >
      cd /workspace/cifar && torchrun --nnodes $NUM_NODES --node_rank $NODE_RANK --master_addr $MASTER_ADDR --master_port 6000 --nproc_per_node $NPROC_PER_NODE main.py \
        --model resnet50 \
        --dataset cifar100 \
        --batch-size 256 \
        --log-interval 100 \
        --total-epochs 50 \
        --save-interval 100 \
        --test-interval 100 \
        --num-dataloader-workers 4 \
        --data-path /workspace/data \
        --save /workspace/ckpt \
        --load /workspace/ckpt
```

## Job 모니터링

[공통 매뉴얼](./common_step.md#job-모니터링)을 참고 바랍니다.

## Checkpoint 다운로드

[공통 매뉴얼](./common_step.md#checkpoint-다운로드)을 참고 바랍니다.
