# HuggingFace를 활용한 Text-Classification 모델 학습하기

본 예시에서는 [HuggingFace](https://github.com/huggingface/transformers)에서 제공하는 Trainer class를 활용하여 클라우드 머신에서 학습을 수행하는 방법을 설명합니다. 예시의 코드는 HuggingFace에서 기본적으로 제공하는 [예시](https://github.com/huggingface/transformers/blob/main/examples/pytorch/text-classification/run_glue.py)를 참고하여 작성되었고 [SDK 예제 레포](https://github.com/friendliai/periflow-python-sdk/tree/main/examples/huggingface)에서 확인 가능합니다.

- `pip install periflow-cli`로 CLI 패키지를 설치합니다.
- `pip install periflow_sdk`로 SDK 패키지를 설치합니다.
- [공통 가이드](./common_step.md)에 설명된 과정들이 완료되어야 합니다.
- 본 튜토리얼에서는 [MNLI](https://cims.nyu.edu/~sbowman/multinli/) dataset을 사용합니다. 다음과 같은 스크립트를 통해서 간단하게 로컬에 dataset을 받을 수 있습니다.

  ```sh
  $ pip install datasets
  $ python -c "from datasets import load_dataset; load_dataset('glue', 'mnli', cache_dir='./mnli')"
  ```

  다운로드가 완료되면 [Dataset 생성](./common_step.md#dataset-생성) 매뉴얼을 따라 데이터셋을 생성 합니다.

## SDK 적용

[run_glue.py](https://github.com/friendliai/periflow-python-sdk/blob/main/examples/huggingface/run_glue.py)에는 HuggingFace를 활용한 학습 코드에 PeriFlow SDK가 적용되어 있습니다. HuggingFace의 Trainer는 임의의 callback을 커스터마이즈 할 수 있기 때문에, PeriFlow의 callback을 다음과 같이 커스터마이즈 할 수 있습니다.

```python
class PeriFlowCallback(TrainerCallback):
    def on_step_begin(self, args, state, control, **kwargs):
        pf.start_step()

    def on_step_end(self, args, state, control, **kwargs):
        pf.end_step()

    def on_save(self, args, state, control, **kwargs):
        pf.upload_checkpoint()
```

`PeriFlowCallback`을 사용하여 HuggingFace의 Trainer를 만들고, 학습을 진행할 수 있습니다.

```python
    pf.init(total_train_steps=training_args.max_steps)
    callback = PeriFlowCallback()

    # Initialize our Trainer
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset if training_args.do_train else None,
        eval_dataset=eval_dataset if training_args.do_eval else None,
        compute_metrics=compute_metrics,
        tokenizer=tokenizer,
        data_collator=data_collator,
        callbacks=[callback],
    )
```

## Configuration YAML 파일

MNLI dataset을 업로드 했고, 로컬에 SDK가 적용된 코드가 준비되었다면, 마지막으로 YAML 파일로 Job의 세부사항을 명시하는 과정만이 남았습니다. [`pf-template.yml`](https://github.com/friendliai/periflow-python-sdk/blob/main/examples/huggingface/pf-template.yml)에는 `run_glue.py`를 수행하기 위한 configuration 예시가 나와 있습니다.

```yaml
# The name of job
name: mnli-job

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
      cd /workspace/huggingface && pip install -r requirements.txt && torchrun --nnodes $NUM_NODES --node_rank $NODE_RANK --master_addr $MASTER_ADDR --master_port 6000 --nproc_per_node $NPROC_PER_NODE run_glue.py \
        --model_name_or_path bert-base-cased \
        --task_name MNLI \
        --max_seq_length 128 \
        --output_dir /workspace/ckpt \
        --do_train \
        --max_steps 500 \
        --save_steps 100 \
        --logging_steps 10 \
        --cache_dir /workspace/data/mnli \
        --logging_dir /workspace/runs
  # Path to mount your workspace volume
  workspace:
    mount_path: /workspace

# Checkpoint config
checkpoint:
  # Path to output checkpoint
  output_checkpoint_dir: /workspace/ckpt

# Configuration dataset
data:
  name: mnli
  mount_path: /workspace/data
```

각 필드에 대한 설명은 [이미지 분류 모델 학습하기](./pytorch_training.md#configuration-yaml-파일)에서 확인할 수 있습니다.

## Job 실행

이제 모든 준비가 완료되었습니다. 우리는 앞에서 `mnli` dataset을 생성하였고, 로컬에는 PeriFlow SDK가 적용된 `run_glue.py`와 configuration YAML 파일 `pf-template.yml`이 준비 되었습니다.

```sh
# 현재 로컬의 디렉토리 구조
$ tree
.
├── pf-template.yml
└── huggingface
    └── run_glue.py
```

이제 다음 커맨드로 Job을 실행합니다.

```sh
pf job run -f pf-template.yml -d ./huggingface
```

## Job 모니터링

[공통 매뉴얼](./common_step.md#job-모니터링)을 참고 바랍니다.

## Checkpoint 다운로드

[공통 매뉴얼](./common_step.md#checkpoint-다운로드)을 참고 바랍니다.
