# **Лекция №17: Обнаружение сдвигов в данных**
> _MLOps_

## **Задание:**
Домашнее задание
Анализ качества и очистка датасета мошеннических финансовых операций

Цель:
В данном домашнем задании вы познакомитесь с основными проблемами, которые могут встречаться в данных. Потренируетесь определять их наличие в датасете, обрабатывать и очищать набор данных. Разрабатывать скрипты очистки данных с использованием Apache Spark.


Описание/Пошаговая инструкция выполнения домашнего задания:
Подробное описание домашнего задания можно найти по ссылке https://github.com/OtusTeam/MLOps/tree/main/hw_03


Критерии оценки:
«Принято» — задание выполнено полностью.
«Возвращено на доработку» — задание не выполнено полностью.

---

## **Выполнено:**

### п.п. 1-2 выполнены аналогично [ДЗ №2](../hw02/README.md)

### 3. Создан Spark кластер, 
указав ранее созданный bucket `s3://otus-bucket-d96c6f8b8b8aa500/` и двумя подкластерами, задав в terraform.tfvars: 
~~~terraform
dataproc_master_resources = {
  resource_preset_id = "s3-c2-m8"
  disk_type_id       = "network-ssd"
  disk_size          = 40
}
dataproc_data_resources = {
    resource_preset_id = "s3-c4-m16"
    disk_type_id       = "network-ssd"
    disk_size          = 128
}
~~~

### 4. Проанализирован датасет мошеннических транзакций на наличие в нем ошибочных данных.

[notebooks/analyze.ipynb](hw03/notebooks/analyze.ipynb)

#### Выявлены следующие ошибочные данные:
- пропуски в колонке terminal_id
- отрицательные идентификаторы в колонке customer_id
- нулевые значения в колонках tx_amount и tx_time_seconds
- нулевые значения в id - колонках  transaction_id и customer_id
- дубли в колонке transaction_id


#### 5. Создан скрипт очистки данных на основе проведенного анализа качества с использованием Apache Spark. 

[clean_dataset.py](scripts/clean_dataset.py)

#### 6. Выполнена очистка датасета с использованием созданного скрипта и сохранение его в созданном выше bucket'е в формате parquet, подходящем для хранения большого объема структурированных данных.

```s3://otus-bucket-d96c6f8b8b8aa500/dataset.parquet```

### 7. Изменен статус задач на Kanban-доске в GitHub Projects в соответствии с достигнутыми результатами. 

### 8. Полностью удален созданный кластер, чтобы избежать оплаты ресурсов в период его простаивания.

---
#### Команды:
~~~bash
cd ../hw02/mlops-otus-practice-cloud-infra/infra
terraform import yandex_storage_bucket.data_bucket otus-bucket-d96c6f8b8b8aa500
cd -
~~~

~~~bash
cd ../hw02/mlops-otus-practice-cloud-infra/infra
terraform state rm yandex_storage_bucket.data_bucket
terraform destroy --auto-approve
cd -
~~~

~~~bash
yc compute instance list --folder-id=b1gp9ivp5r63jjvecumi
~~~

~~~bash
#otus-proxy-vm
ssh -o IdentitiesOnly=yes ubuntu@$(yc compute instance list --folder-id=b1gp9ivp5r63jjvecumi | grep otus-proxy-vm |  awk '{print $10}' | head -n 1) -i ~/.ssh/.ssh_otus_mlops
~~~

~~~bash
#rc1a-dataproc-m
ssh -o IdentitiesOnly=yes ubuntu@$(yc compute instance list --folder-id=b1gp9ivp5r63jjvecumi | grep rc1a-dataproc-m |  awk '{print $10}' | head -n 1) -i ~/.ssh/.ssh_otus_mlops
~~~

~~~bash
#otus-proxy-vm
ssh -o IdentitiesOnly=yes -L 8888:localhost:8888 ubuntu@$(yc compute instance list --folder-id=b1gp9ivp5r63jjvecumi | grep otus-proxy-vm |  awk '{print $10}' | head -n 1) -i ~/.ssh/.ssh_otus_mlops
~~~

~~~bash
#rc1a-dataproc-m
ssh -o IdentitiesOnly=yes -L 8888:localhost:8888 ubuntu@$(yc compute instance list --folder-id=b1gp9ivp5r63jjvecumi | grep rc1a-dataproc-m |  awk '{print $10}' | head -n 1) -i ~/.ssh/.ssh_otus_mlops
~~~

~~~bash
jupyter notebook --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password=''
~~~
