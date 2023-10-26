* DE_TASK

```
git clone https://github.com/mitnitskiyas-kz/de_task.git
pip install -r requirements.txt
pip install -e .
```

Edit cfg/config.yaml

Fill in kaggle_integration username:

```
kaggle_integration:
  username: "alekseimitnitskiy"
```


```
python main.py cfg/config.yaml key_kaggle
```

