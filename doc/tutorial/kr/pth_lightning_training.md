# PyTorch Lightning을 활용한 AutoEncoder 모델 학습하기

본 예시에서는 [PyTorch Lightning](https://www.pytorchlightning.ai/)을 활용하여 클라우드 머신에서 학습을 수행하는 방법을 설명합니다. 예시의 코드는 PyTorch Lightning에서 기본적으로 제공하는 [예시](https://github.com/PyTorchLightning/pytorch-lightning/blob/master/pl_examples/basic_examples/autoencoder.py)를 참고하여 작성되었고 [SDK 예제 레포](https://github.com/friendliai/periflow-python-sdk/tree/main/examples/pth-lightning)에서 확인 가능합니다.

- `pip install periflow-cli`로 CLI 패키지를 설치합니다.
- `pip install periflow_sdk`로 SDK 패키지를 설치합니다.
- [공통 가이드](./common_step.md)에 설명된 과정들이 완료되어야 합니다.
- 본 튜토리얼에서는 [MNIST](https://cims.nyu.edu/~sbowman/multinli/) dataset을 사용합니다.
다음과 같은 스크립트를 통해서 간단하게 로컬에 dataset을 받을 수 있습니다.
  ```sh
  $ pip install torchvision
  $ python -c "from torchvision import datasets; datasets.MNIST('./', download=True)"
  ```
  이후 `pf datastore upload`를 통해서 local의 dataset을 업로드할 수 있습니다.

## SDK 적용

[main.py](https://github.com/friendliai/periflow-python-sdk/blob/main/examples/pth-lightning/main.py)에는 PyTorch Lightning을 활용한 학습 코드에 PeriFlow SDK가 적용되어 있습니다. PyTorch Lightning의 Trainer는 임의의 callback을 커스터마이즈 할 수 있기 때문에, PeriFlow의 callback을 다음과 같이 커스터마이즈 할 수 있습니다.

```python
class PeriFlowCallback(Callback):
    def on_train_batch_start(self,
                             trainer: pl.Trainer,
                             pl_module: pl.LightningModule,
                             batch: Any,
                             batch_idx: int,
                             unused: int = 0) -> None:
        pf.start_step()

    def on_train_batch_end(self,
                           trainer: pl.Trainer,
                           pl_module: pl.LightningModule,
                           outputs: STEP_OUTPUT,
                           batch: Any,
                           batch_idx: int,
                           unused: int = 0) -> None:
        loss = float(outputs['loss'])
        pf.metric({
            "iteration": trainer.global_step,
            "loss": loss,
        })
        pf.end_step()
```

PyTorch Lightning은 checkpoint 저장 이후에 대한 callback을 지원하지 않아서 다음과 같이 간단하게 PyTorch Lightning의 Trainer를 커스터마이즈 할 수 있습니다.

```python
class PeriFlowTrainer(Trainer):
    def save_checkpoint(self,
                        filepath: Union[str, Path],
                        weights_only: bool = False,
                        storage_options: Optional[Any] = None) -> None:
        super().save_checkpoint(filepath, weights_only=weights_only, storage_options=storage_options)
        pf.upload_checkpoint()
```

PeriFlowCallback과 PeriFlowTrainer를 통해서 다음과 같이 학습을 진행할 수 있습니다.

```python
    model = LitAutoEncoder()
    datamodule = MyDataModule()
    pf.init(total_train_steps=args.num_epochs * datamodule.num_steps_per_epoch)

    periflow_callback = PeriFlowCallback()
    trainer = PeriFlowTrainer(
        max_epochs=args.num_epochs,
        callbacks=[periflow_callback, checkpoint_callback],
        enable_checkpointing=isinstance(checkpoint_callback, ModelCheckpoint),
    )
```

## Configuration YAML 파일

Datastore에 MNIST dataset을 업로드 했고, 로컬에 SDK가 적용된 코드가 준비되었다면, 마지막으로 YAML 파일로 Job의 세부사항을 명시하는 과정만이 남았습니다. [`pf-template.yml`](https://github.com/friendliai/periflow-python-sdk/blob/main/examples/pth-lightning/pf-template.yml)에는 `main.py`를 수행하기 위한 configuration 예시가 나와 있습니다.

```yaml
# The name of experiment
experiment: mnist-autoencoder

# The name of job
name: autoencoder-job

# The name of vm type
vm: azure-16gb-v100-1g-eastus-spot

# The number of GPU devices
num_devices: 1

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
    #   - NUM_NODES: number of total nodes.
    #   - NPROC_PER_NODE: number of devices per node.
    command: >
      cd /workspace/pth-lightning && pip install pytorch-lightning && python main.py \
        --checkpoint-dir /workspace/ckpt \
        --num-epochs 10
  # Path to mount your workspace volume
  workspace:
    mount_path: /workspace

# Checkpoint config
checkpoint:
  # Path to output checkpoint
  output_checkpoint_dir: /workspace/ckpt
```

각 필드에 대한 설명은 [single machine training 예시](https://github.com/friendliai/periflow-cli/blob/tutorial-md/doc/tutorial/kr/single_machine_training.md)에서 확인할 수 있습니다.

## Job 실행

이제 모든 준비가 완료되었습니다. 우리는 앞에서 `mnli` dataset을 생성하였고, 로컬에는 PeriFlow SDK가 적용된 `run_glue.py`와 configuration YAML 파일 `pf-template.yml`이 준비 되었습니다.

```sh
# 현재 로컬의 디렉토리 구조
$ tree
.
├── pf-template.yml
└── pth-lightning
    └── main.py
    └── environment.yml
    └── mnist_datamodule.py
    └── utils.py
```

이제 다음 커맨드로 Job을 실행합니다.

```sh
pf job run -f pf-template.yml -d ./pth-lightning
```

## Job 모니터링

## Checkpoint 다운로드

[공통 매뉴얼](./common_step.md#checkpoint-다운로드)
