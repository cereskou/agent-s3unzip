# agent-s3unzip
## AWS Lambda function
this function will unzip s3 file to another bucket. (pair of agent-s3zip)

### Create AWS Lambda Function
![create](https://github.com/cereskou/agent-s3unzip/blob/main/images/lambda-create.png)
1. Create a lambda function named **S3ServiceUnTar**.<br>
1. Upload the main.zip(built by gobuild.cmd).
1. change the function setup. 

![setup](https://github.com/cereskou/agent-s3unzip/blob/main/images/lambda-set.png)

### Setup S3 bucket event (sorry for japanese)
1. AWSコンソールページに、「S3バケット」を選択します。
1. 「イベント」ボタンをクリックします。
1. 「通知の追加」をクリックして、入力画面を表示します。
1. 名前が「S3ServiceUntar」を入力します。（名前が任意）
1. イベントが「PUT」をチェックします。
1. プレフィックスが「作業用パス」を入力します。（ファイル一覧.jsonファイルのアップロード先）
1. サフィックスが **.json** を入力します。
1. 送信先が「Lambda関数」を選択します。
1. Lambda関数 **S3ServiceUntar**を選択し、保存します。
![s3bucket](https://github.com/cereskou/agent-s3unzip/blob/main/images/s3bucket.png)

### Function flow
![flow](https://github.com/cereskou/agent-s3unzip/blob/main/images/zip-flow.png)

### JSON

```
//File -
type File struct {
	Err         error     `json:"-"`
	Bucket      string    `json:"bucket"`
	Key         string    `json:"key"`
	Local       string    `json:"local"`
	Relative    string    `json:"-"` //相対パス（ZIP用）
	IsDir       bool      `jong:"isdir"`
	Size        int64     `json:"size"`
	Md5         string    `json:"md5"`
	ContentType string    `json:"contentType"`
	ModTime     time.Time `json:"modtime"`
	Num         uint32    `json:"num"`
	Offset      int64     `json:"offset"`
	Length      int64     `json:"length"`
	Total       int64     `json:"total"`
	UploadID    string    `json:"uploadId"`
}

//Package - put job/ get job
type Package struct {
	ID       int64   `json:"id"`       //Package id
	Tag      string  `json:"tag"`      //identity tag for client's job
	Dir      string  `json:"dir"`      //save to
	S3       string  `json:"s3"`       //s3 url
	Bucket   string  `json:"bucket"`   //upload to s3 bucket
	Prefix   string  `json:"prefix"`   //upload to s3 prefix
	Work     string  `json:"-"`        //workdirectory
	Md5      bool    `json:"md5"`      //md5 check flag
	Count    int64   `json:"count"`    //Data count
	PartSize int64   `json:"partsize"` //multi-part size
	Data     []*File `json:"data"`     //data array
}
```
