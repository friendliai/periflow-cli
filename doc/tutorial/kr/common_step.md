# PeriFlow 튜토리얼

PeriFlow를 사용하시는 여러분들을 환영합니다. 본 튜토리얼에서는 PeriFlow 사용을 위한 방법을 단계별로 설명합니다. 튜토리얼의 내용과 관련하여 궁금한 점이 있으시다면 언제든지 [PeriFlow Discussion Forum](https://discuss-staging.friendli.ai/)에 질문을 남겨주시기 바랍니다.

## Basics

본격적인 튜토리얼에 앞서 다음과 같은 PeriFlow의 용어/개념들을 알고 계시면 좋습니다.

<p align="center">
  <img src="../../assets/group_hierarchy.png" width="70%" alt="system">
</p>

- **Organization**: PeriFlow의 최상위 그룹 단위입니다. 조직 내에 멤버들을 초대하여 개발 및 실험 과정을 협업할 수 있습니다. 사용자는 동시에 여러 개의 Organization에 속할 수 있습니다.
- **Project**: Organization 내의 서브 그룹의 단위입니다. Organization 내에서 용도에 따라 여러 개의 Project를 생성하고 각 Project 마다 필요한 멤버들을 추가할 수 있습니다. Project 내에서는 Job, Dataset, Checkpoint, Credential 등의 모든 자원이 공유됩니다. 사용자는 동시에 여러 개의 Project에 속할 수 있습니다.
- **Job**: 학습 작업의 실행/스케줄링 단위 입니다. `python main.py ...`와 같은 명령어를 사용하여 학습 프로세스를 실행하는 것이 PeriFlow에선 Job 하나에 해당 합니다.
- **Dataset**: Job에 사용될 학습 데이터입니다.
- **Checkpoint**: 모델 학습의 결과물로 생긴 모델 가중치 체크포인트입니다. 학습 과정의 특정 스텝에서 Checkpoint 하나가 생성될 수 있습니다.
- **Credential**: 유저의 개인 클라우드 저장소, Slack 등에 접근하기 위해 필요한 secret 입니다.

## Requirements

- `pip install periflow-cli`를 통해 컴퓨터에 `periflow-cli` 패키지를 설치해야 합니다.

## 사용 방법 설명

1. [회원가입](#sign-up)
2. [로그인](#login)
3. [Organization 관리](#organization-관리)
4. [Project 관리](#project-관리)
5. [Credential 생성](#crdential-생성)
6. [Dataset 생성](#dataset-생성)
7. [Job 실행](#job-실행)
8. [Job 모니터링](#job-모니터링)
9. [Checkpoint 다운로드](#checkpoint-다운로드)

### Sign-Up

PeriFlow에 사용자 계정을 생성하고, Organization 및 Project를 생성하여 여러 멤버들과 함께 작업이 가능합니다.

```sh
pf signup
```

위의 명령어를 실행하면 프롬프트를 통해 username, name, email, password를 입력한 뒤 이메일로 전송된 인증 코드를 입력하여 회원가입을 진행할 수 있습니다.

### Login

PeriFlow를 사용하기 위해서 우선 로그인을 해야합니다. `pf login` 커맨드를 사용하여 유저 이름과 패스워드를 쳐서 로그인을 합니다.

```sh
pf login
```

로그인 이후엔 현재 나의 "작업 context"를 설정할 수 있습니다. 여기에서 "작업 context"란 현재 내가 작업을 할 Organization 및 Project가 무엇인지에 해당합니다.
아래 커맨드를 사용하여 작업할 Organization을 지정할 수 있습니다. Organization들의 리스트를 보려면 `pf org list` 커맨드를 사용하시면 됩니다.

> **만약 Organization과 Project에 생성/초대 과정을 거치지 않았다면 [Organization 관리](#organization-관리) 및 [Project 관리](#project-관리) 항목을 먼저 읽어 주세요.**

```sh
pf org switch [ORGANIZATION_NAME]
```

또한 아래 커맨드를 사용하여 작업할 Project를 지정할 수 있습니다. 이 때 현재 선택된 Organization 내의 Project만 선택이 가능합니다. Project들의 리스트를 보려면 `pf project list` 커맨드를 사용하시면 됩니다.

```sh
pf project switch [PROJECT_NAME]
```

### Organization 관리

PeriFlow 서비스 이용을 위해서는 적어도 하나의 Organization에 속해있어야 합니다. 사용자는 직접 Organization을 생성하거나, 다른 사용자가 초대한 Organization에 들어감으로써 Organization에 속할 수 있습니다.

#### Organization 생성

```sh
pf org create [ORGANIZATION_NAME]
```

위의 명령어를 통해 새로운 Organization을 생성할 수 있습니다.

> **Organization 생성 직후엔 "Staged" 상태로, 서비스 관리자의 승인을 거쳐 "Active" 상태가 될 때까지 이용이 불가능합니다.**

#### Organization 초대

Organzation 생성 및 승인을 거쳐 Organization이 "Active" 상태가 되었다면 다른 사용자를 Organization에 초대할 수 있습니다.

```sh
pf org invite [EMAIL_ADDRESS]
```

위의 명령어를 통해 해당 이메일 주소로 Organization 초대 메일이 보내지고, 초대를 받은 사용자는 이메일로 전달된 링크 또는 승인 코드를 통해 초대를 수락할 수 있습니다.

#### Organization 초대 수락

```sh
pf org accept-invite
```

Organization에 초대를 받은 사용자는 자신의 이메일로 전송된 링크 또는 승인 코드를 통해 초대를 수락할 수 있습니다.

#### Organization 내 권한 부여

Organization에는 "Owner"와 "Member" 두 개의 역할이 있고 각 역할은 다음과 같은 권한을 보유합니다.

|                          | Owner | Member |
|--------------------------|-------|--------|
| Organization 내 서비스 이용 |   O   |    O   |
| Member 초대               |   O   |    X   |
| Organization 내 권한 부여   |  O    |   X    |
| Project 생성 및 삭제        |  O    |   X    |
| 서비스 이용 요금 지불         |  O    |   X    |
| Organization 삭제         |  O    |   X    |

Organization을 생성한 사용자는 기본으로 Owner가 되며, Organization에 초대된 사용자는 기본으로 Member가 됩니다.
Owner는 위의 명령어를 통해 Organization에 속한 특정 사용자의 역할을 수정할 수 있습니다.

```sh
pf org set-role [USERNAME] [ROLE]
```

### Project 관리

Organization 아래에는 여러 개의 Project들이 존재합니다. 각각의 Project 내에서는 구성원 간에 Dataset, Checkpoint, Artifact, Job 등의 자원 및 서비스들이 공유됩니다.

#### Project 생성

```sh
pf project create [PROJECT_NAME]
```

위의 명령어를 통해 새로운 Project를 생성할 수 있습니다.

#### Project 초대

```sh
pf project add-user [USERNAME] [ROLE]
```

위의 명령어를 통해 같은 Organization에 속한 사용자를 Project에 추가하고 역할을 설정할 수 있습니다.

#### Project 내 권한 부여

Project에는 "Admin", "Maintainer", "Developer", "Guest" 4개의 역할이 존재합니다.

```sh
pf set-role [USERNAME] [ROLE]
```

위의 명령어를 사용하여 특정 사용자에게 원하는 역할을 할당할 수 있습니다.

### Crdential 생성

PeriFlow를 본격적으로 사용하기에 앞서 필요한 credential들을 미리 등록해두면 좋습니다. 현재 등록 가능한 Credential의 종류는 다음과 같습니다.

- [Docker](#docker)
- [AWS S3](#aws-s3)
- [Azure Blob Storage](#azure-blob-storage)
- [Google Cloud Storage](#google-cloud-storage)
- [Weights & Biases](#weights--biases)

본인에게 필요한 Credential이 있다면 아래의 설명을 참고하여 생성을 합니다.

> **Credential 생성을 하지 않더라도 튜토리얼의 내용을 따라하는데 문제가 없습니다.**

#### Docker

개인 Docker Hub 계정에 있는 도커 이미지에 접근하기 위해 필요합니다. Private으로 설정된 Docker Hub 레포지토리에 있는 이미지를 Job에서 사용하기 원한다면 등록해야 합니다.

```sh
pf credential create -n [CREDENTIAL_NAME] --username [USERNAME] --password [PASSWORD]
```

- USERNAME: Docker Hub 로그인 시 사용하는 username
- PASSWORD: Docker Hub 로그인 시 사용하는 password

> 만약 Organization 내의 다른 멤버들과 Credential을 공유하고 싶다면 `-g` 또는 `--group` 옵션을 붙여주세요.

#### AWS S3

개인 AWS 계정 S3 버켓에 업로드 된 있는 데이터셋, 체크포인트 파일에 접근하기 위해 필요합니다.

```sh
pf credential create s3 \
    -n [CREDENTIAL_NAME] \
    --aws-access-key-id [AWS_ACCESS_KEY_ID] \
    --aws-secret-access-key [AWS_SECRET_ACCESS_KEY] \
    --aws-default-region [AWS_DEFAULT_REGION]
```

본인의 `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`이 무엇인지 알기 원한다면 다음 과정을 따릅니다.

1. 웹브라우저를 통해 AWS Console 사이트에 접속합니다.
2. 내가 사용하고 싶은 S3 버켓이 존재하는 AWS 계정에 로그인 합니다.
3. [Security credential 페이지](https://console.aws.amazon.com/iam/home?#security_credential)에 접속한 뒤, "AWS IAM credentials" 탭에서 "Create access key" 버튼을 클릭하여 새로운 엑세스 키를 생성하면 Access key ID (e.g., `BKIYTQTRK42VYNAWYI7S`) 와 Secret access key(e.g., `KZ1itFd0vkXI0awoPtB1Z0R+CGRFFHiS1a5K4A1C`)를 얻을 수 있습니다.
4. 위의 커맨드에서 `AWS_ACCESS_KEY_ID`에는 Access Key ID(e.g., `BKIYTQTRK42VYNAWYI7S`)를, `AWS_SECRET_ACCESS_KEY`에는 Secret access key(e.g., `KZ1itFd0vkXI0awoPtB1Z0R+CGRFFHiS1a5K4A1C`)를 입력합니다.
5. `AWS_DEFAULT_REGION`에는 본인이 주로 사용할 AWS 리전의 코드를 입력합니다. 리전 코드는 [테이블](https://docs.aws.amazon.com/ko_kr/AWSEC2/latest/UserGuide/using-regions-availability-zones.html#concepts-regions)을 참고 바랍니다.

> 만약 Organization 내의 다른 멤버들과 Credential을 공유하고 싶다면 `-g` 또는 `--group` 옵션을 붙여주세요.

#### Azure Blob Storage

개인 Azure 계정 Blob 컨테이너에 업로드 된 데이터셋, 체크포인트 파일에 접근하기 위해 필요합니다. Azure Blob Storage는 storage account 하위에 여러 개의 container가 존재하는 형태로 구성되어 있습니다. 본 섹션에서 설명하는 과정을 진행하면 storage account 하위에 존재하는 모든 container에 접근할 수 있게 됩니다.

```sh
pf credential create azure-blob \
    -n [CREDENTIAL_NAME] \
    --storage-account-name [STORAGE_ACCOUNT_NAME] \
    --storage-account-key [STORAGE_ACCOUNT_KEY]
```

본인의 `STORAGE_ACCOUNT_NAME`, `STORAGE_ACCOUNT_KEY`가 무엇인지 알기 원한다면 다음 과정을 따릅니다.

1. 웹브라우저를 통해 Azure Portal에 접속합니다.
2. "Storage accounts" 페이지로 이동하여 내가 사용할 storage account를 클릭합니다.
3. 네비게이션 탭에서 "Access keys"를 클릭합니다.
4. 2개의 key (key1, key2) 두 개가 보이는데, 이 중에서 하나를 선택하여 "Key"에 있는 값을 복사합니다. (무엇을 선택하든 상관 없습니다.)
5. 위의 커맨드에서 `STORAGE_ACCOUNT_NAME`에는 storage account의 이름(e.g, `mystorageaccount`)을, `STORAGE_ACCOUNT_KEY`에는 앞 단계에서 얻은 Key의 값(e.g., `+Za94cnkkRpZLf8S5fdngAK8eBu/CkUJDpl8u0k+Lo1zyazvAXXca1q3JajHv33cFmv9F7f0Cz1iPKtRd4zJzSQ==`)을 입력합니다.

> 만약 Organization 내의 다른 멤버들과 Credential을 공유하고 싶다면 `-g` 또는 `--group` 옵션을 붙여주세요.

#### Google Cloud Storage

Google Cloud Storage 버켓에 업로드 된 데이터셋에 접근하기 위해 필요합니다.

```sh
pf credential create gcs \
    -n [CREDENTIAL_NAME] \
    --service-account-key-file [SERVICE_ACCOUNT_KEY_FILE]
```

본인의 `SERVICE_ACCOUNT_KEY_FILE`을 얻기 위해서 다음 과정을 따릅니다.

1. 웹브라우저를 통해 Google Cloud Platform Console에 접속합니다.
2. "서비스 계정(Service Account)" 페이지로 이동합니다.
3. 사용할 버켓이 존재하는 Project를 선택합니다.
4. 키를 만들려는 서비스 계정의 이메일 주소를 클릭합니다.
5. "키(Keys)" 탭을 클릭하여 이동합니다.
6. "키 추가(Add Key)" 드롭다운 메뉴를 클릭한 후 "새 키 만들기(Create new key)"를 선택합니다.
7. "키 유형(Key type)"으로 JSON을 선택하고 "만들기(Create)"를 클릭합니다.
8. "만들기(Create)"를 클릭하면 아래와 같은 형식의 서비스 계정의 키 파일이 다운로드 됩니다.
9. 위 커맨드에서 `SERVICE_ACCOUNT_KEY_FILE`에는 다운로드 된 파일의 경로를 입력합니다.

```json
{
  "type": "service_account",
  "project_id": "project-id",
  "private_key_id": "key-id",
  "private_key": "-----BEGIN PRIVATE KEY-----\nprivate-key\n-----END PRIVATE KEY-----\n",
  "client_email": "service-account-email",
  "client_id": "client-id",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://accounts.google.com/o/oauth2/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/service-account-email"
}
```

> 만약 Organization 내의 다른 멤버들과 Credential을 공유하고 싶다면 `-g` 또는 `--group` 옵션을 붙여주세요.

#### Weights & Biases

Weight & Biases를 사용하여 학습 메트릭을 모니터링 하려면 Weights & Biases 계정 접근을 위한 API 키를 등록해야 합니다.

```sh
pf credential create wandb \
    -n [CREDENTIAL_NAME] \
    --api-key [API_KEY]
```

Weights & Biases API key 값을 얻으려면 다음 과정을 따릅니다.

1. 브라우저를 통해 Weights & Biases에 접속하여 로그인 합니다.
2. "Settings" 페이지로 이동합니다.
3. "API keys" 패널에서 "New key"를 클릭하여 키를 생성하고 복사합니다.
4. 복사한 키를 위의 커맨드에서 `API_KEY` 부분에 입력합니다.

> 만약 Organization 내의 다른 멤버들과 Credential을 공유하고 싶다면 `-g` 또는 `--group` 옵션을 붙여주세요.

### Dataset 생성

PeriFlow에 Dataset을 생성하는 방법은 2가지가 있습니다.

1. [컴퓨터 로컬 파일 시스템에 있는 데이터셋 파일을 업로드](#from-local-file-system)
2. [클라우드 스토리지에 업로드 된 데이터셋을 연결](#from-cloud-storage)

#### From Local File System

로컬 파일 시스템에 있는 파일을 업로드 하는 방법은 간단합니다. 예를 들어, 내 컴퓨터에 다음과 같이 업로드할 데이터셋이 존재한다고 해봅시다.

```sh
$ tree 
.
└── cifar100
    └── cifar-100-python
        ├── meta
        ├── test
        └── train

```

우리는 이 중에 `cifar100` 디렉토리 안에 있는 `cifar-100-python` 디렉토리를 업로드하여 Dataset으로 사용하고 싶습니다. 이를 위해서 우리는 단 한 줄의 커맨드만 입력하면 됩니다.

```sh
$ pf dataset upload -n my-cifar-100 -p ./cifar100/cifar-100-python

# 또는

$ pf dataset upload -n my-cifar-100 -p ./cifar100/
```

- `-n` 옵션에는 데이터셋의 이름을 입력합니다. 데이터셋의 이름은 Organization 내에서 유일해야 합니다 (Organization 내에서 중복 이름 사용 불가).
- `-p` 옵션에는 업로드할 로컬에 있는 데이터셋 경로를 입력합니다. 이 때 경로가 `/`로 끝나게 되면 해당 디렉토리 내부에 있는 파일/디렉토리들만 업로드 됩니다. 예를 들어 위의 예시에서 `-p ./cifar100/cifar-100-python`로 옵션을 준 경우, `cifar-100-python` 디렉토리를 포함하여 그 내부의 파일들이 업로드 되고, `-p ./cifar100/cifar-100-python/`로 옵션을 준 경우엔 `cifar-100-python` 디렉토리는 제외하고 그 내부의 파일들(`meta`, `test`, `train`)만 업로드 됩니다.

업로드 커맨드를 입력하면 상태 바가 나타나면서 업로드가 진행됩니다. 만약 업로드할 파일 사이즈가 크다면 `tmux`, `screen` 세션 등을 사용하여 업로드하는 것을 권장합니다.

업로드가 완료되었다면 `pf dataset view my-cifar-100` 커맨드를 통해 업로드한 데이터셋의 세부 사항을 확인할 수 있습니다.

```sh
$ pf dataset view my-cifar-100  
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

#### From Cloud Storage

PeriFlow에서 사용하고 싶은 데이터셋이 이미 클라우드 스토리지(e.g., AWS S3, Azure Blob Storage, Google Cloud Storage)에 업로드 된 상태라면 별도의 업로드 과정 없이 해당 스토리지를 링크하여 사용이 가능합니다. 이를 위해서는 데이터셋이 업로드 된 클라우드 스토리지에 접근 권한이 필요하기 때문에 [Credential 생성](#crdential-생성) 설명을 참고하여 Credential이 생성된 상태여야 합니다.

예를 들어, AWS `us-east-1` 리전에 `cifar-100`이라는 이름의 S3 버켓이 있고, 우리는 이 버켓에 있는 데이터셋을 PeriFlow에 등록하고 싶은 상황이라고 해보겠습니다. 위의 [매뉴얼](#aws-s3)을 따라 AWS S3 Credential을 생성하고 다음과 같이 커맨드를 실행합니다.

```sh
$ pf dataset create \
    -n aws-cifar-100 \
    -c s3 \
    -r us-east-1 \
    -s cifar-100 \
    -i f2a357dc-2029-454d-aa37-73ebdbc5ea43
```

- `-n` 옵션에는 데이터셋의 이름을 입력합니다. 데이터셋의 이름은 Organization 내에서 유일해야 합니다 (Organization 내에서 중복 이름 사용 불가).
- `-s` 옵션에는 링크할 클라우드 스토리지 타입을 입력합니다. 본 예시에서는 S3 버켓을 연결하므로 `s3`를 입력합니다.
- `-r` 옵션에는 클라우드 스토리지의 리전을 입력합니다. 본 예시에서 사용하는 S3 버켓은 `us-east-1` 리전에 있으므로 `us-east-1`을 입력합니다.
- `-s` 옵션에는 클라우드 스토리지의 이름을 입력합니다. 본 예시에서 사용하는 S3 버켓의 이름은 `cifar-100`이므로 `cifar-100`을 입력합니다.
- `-i` 옵션에는 S3 타입의 Credential ID(UUID 형식)를 입력합니다.

업로드가 완료되었다면 `pf dataset view aws-cifar-100` 커맨드를 통해 업로드한 데이터셋의 세부 사항을 확인할 수 있습니다.

```sh
$ pf dataset view aws-cifar-100  
╭─────────────────────────── Overview ───────────────────────────╮
│  Name          aws-cifar-100                                   │
│  Cloud         s3                                              │
│  Region        us-east-1                                       │
│  Storage Name  cifar-100                                       │
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

### Job 실행

실행하려는 Job의 종류에 따라 아래 링크된 매뉴얼을 참고 바랍니다.

1. [이미지 분류 모델 학습하기](./pytorch_training.md)
2. [PyTorch Lightning을 활용한 AutoEncoder 모델 학습하기](./pth_lightning_training.md)
3. [HuggingFace를 활용한 Text-Classification 모델 학습하기](./huggingface_training.md)

### Job 모니터링

`pf job run` 명령어로 Job을 실행하였다면 `pf job log JOB_ID` 커맨드를 통해 Job에서 출력되는 stdout, stderr를 모니터링 할 수 있습니다.

### Checkpoint 다운로드

PeriFlow SDK를 학습 스크립트에 적용하고 Job을 성공적으로 실행하였다면, 학습이 진행되면서 생성되는 모델 체크포인트를 실시간으로 확인하고 로컬 파일 시스템으로 다운로드 할 수 있습니다.

`pf job view JOB_ID`를 하면 학습에서 생성된 체크포인트들의 목록을 확인할 수 있습니다. 다운로드 하고 싶은 체크포인트가 있다면 `pf checkpoint download CHECKPOINT_ID` 명령어를 사용합니다.
