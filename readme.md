## 简介
pytest-cassandra插件
## 安装

`pip install pytest-cassandra -i http://*:8082/private_repository/ --trusted-host *`

## 使用
### 测试用例可使用cassandra fixtrue

```python
def test_cassandra(cassandra,):
    data = cassandra['datacollection'].fetch('vehicle_data', {"vehicle_id": '74361e94a61846e2a690d2e2a9bf591d', "sample_date": '2019-10', "msg_type": 'instant_status_resp'})
```
### 运行测试
需编写pytest.ini文件，置于项目内的根目录上，用于指定cassandra配置路径。
默认在项目内的根目录下寻找环境对应配置(./config/config.yml)

####pytest.ini
```ini
[cassandra]
config = config/config.yml
```
或在命令行中通过--config参数指定路径
```bash
pytest --config_cassandra config/config.yml
```
####test_config.yml配置如下:
```yaml
cassandra:
  datacollection:
    nodes:
      - your_db_nodeip
      - your_db_nodeip
      - your_db_nodeip
    port: 9042
    keyspace: data_collection_test
    user: you_user_name
    password: *
```
## 打包
`python setup.py sdist bdist`  
`twine upload -r my_nexus dist/*`