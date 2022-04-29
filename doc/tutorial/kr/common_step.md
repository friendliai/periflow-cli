# PeriFlow 튜토리얼

PeriFlow를 사용하시는 여러분들을 환영합니다. 본 튜토리얼에서는 PeriFlow 사용을 위한 방법을 단계별로 설명합니다. 튜토리얼의 내용과 관련하여 궁금한 점이 있으시다면 언제든지 [PeriFlow Discussion Forum]에 질문을 남겨주시기 바랍니다.

본 문서는 PeriFlow를 사용하는 유저가 공통적으로 거치게 되는 과정을 담고 있습니다. 여러 가지 학습 케이스에 대한 튜토리얼은 본 문서에 설명된 과정을 마친 후 다음 링크의 문서를 참고 바랍니다.

1. [단일 머신을 사용하는 간단한 학습 예시](./single_machine_training.md)

## Basics

본격적인 튜토리얼에 앞서 다음과 같은 PeriFlow의 용어/개념들을 알고 계시면 좋습니다.

- Organization: 여러 유저들이 속한 그룹을 의미합니다. 그룹 내의 유저들은 Dataset, Checkpoint 같은 자원들을 공유할 수도 있습니다.
- Job: 학습의 스케줄링 단위 입니다. 로컬에서 `python main.py ...`와 같이 학습 프로세스를 실행하는 것이 PeriFlow에선 Job 하나에 해당한다고 생각하시면 됩니다.
- Experiment: 여러 Job들의 묶음으로, 동질적인 Job에 붙는 태그와 같습니다.
- Datastore/Dataset: Datastore는 Job에 사용될 여러 Dataset들의 집합입니다.
- Checkpoint: 모델 학습의 결과물로 생긴 모델 가중치 체크포인트입니다. 학습 과정의 특정 스텝에서 Checkpoint 하나가 생성될 수 있습니다.
- Credential: 유저의 개인 클라우드 저장소, Slack 등에 접근하기 위해 필요한 secret 입니다..

## Requirements

- `pip install periflow-cli`를 통해 컴퓨터에 `periflow-cli` 패키지를 설치해두어야 합니다.

## 사용 방법 설명

### 로그인

PeriFlow를 사용하기 위해서 우선 로그인을 해야합니다. `pf login` 커맨드를 사용하여 유저 이름과 패스워드를 쳐서 로그인을 합니다.

```sh
pf login
```

### Crdential 생성

PeriFlow를 본격적으로 사용하기에 앞서 필요한 credential들을 미리 등록해두면 좋습니다. 현재 등록 가능한 Credential의 종류는 다음과 같습니다.

- Docker
- AWS S3
- Azure Blob Storage
- Google Cloud Storage
- Weights & Biases
- Slack

본인에게 필요한 Credential이 있다면 아래의 설명을 참고하여 생성을 합니다.

#### Docker

개인 Docker Hub 계정에 있는 도커 이미지에 접근하기 위해 필요합니다. Private으로 설정된 개인 Docker Hub 레포지토리에 있는 이미지를 Job에서 사용하기 원한다면 등록해야 합니다.

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
