## 上传图片示例

用户上传一次图片，在后端需要做多中类型的操作。使用RabbitMQ广播(`fanout`)的形式实现多任务，并进行解耦。

#### producer_upload_fanout.py

产生上传图片的消息，并广播。

#### consumer_resize.py

接受广播的消息，并且对上传的图片进行修改大小处理。

#### consumer_present_integral.py

接受广播的消息，并且对上传的图片的用户添加积分。
