# Generated by Django 3.0.6 on 2021-05-30 12:13

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0151_auto_20210530_1153'),
    ]

    operations = [
        migrations.AlterField(
            model_name='market',
            name='contract_value',
            field=models.FloatField(blank=True, null=True),
        ),
    ]
