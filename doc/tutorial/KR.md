# PeriFlow 튜토리얼

PeriFlow를 사용하시는 여러분들을 환영합니다. 본 튜토리얼에서는 PeriFlow 사용을 위한 방법을 단계별로 설명합니다. 튜토리얼의 내용과 관련하여 궁금한 점이 있으시다면 언제든지 [PeriFlow Discussion Forum]에 질문을 남겨주시기 바랍니다.

## Overview

본격적인 튜토리얼에 앞서 다음과 같은 PeriFlow의 용어/개념들을 알고 계시면 좋습니다.

- Organization: 여러 유저들이 속한 그룹을 의미합니다. 그룹 내의 유저들은 Dataset, Checkpoint 같은 자원들을 공유할 수도 있습니다.
- Job: 학습의 스케줄링 단위 입니다. 로컬에서 `python main.py ...`와 같이 학습 프로세스를 실행하는 것이 PeriFlow에선 Job 하나에 해당한다고 생각하시면 됩니다.
- Experiment: 여러 Job들의 묶음으로, 동질적인 Job에 붙는 태그와 같습니다.
- Datastore/Dataset: Datastore는 Job에 사용될 여러 Dataset들의 집합입니다.
- Checkpoint: 모델 학습의 결과물로 생긴 모델 가중치 체크포인트입니다. 학습 과정의 특정 스텝에서 Checkpoint 하나가 생성될 수 있습니다.

## Requirements

- `pip install periflow-cli`를 통해 컴퓨터에 `periflow-cli` 패키지를 설치해두어야 합니다.

## Common Workflow

본 튜토리얼에서는 (1) 단일 머신 학습

### 로그인

PeriFlow를 사용하기 위해서 우선 로그인을 해야합니다. `pf login` 커맨드를 사용하여 유저 이름과 패스워드를 쳐서 로그인을 합니다.

```sh
pf login
```
