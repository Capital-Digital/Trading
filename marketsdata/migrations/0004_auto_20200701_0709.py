# Generated by Django 3.0.6 on 2020-07-01 07:09

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0003_auto_20200701_0709'),
    ]

    operations = [
        migrations.AlterField(
            model_name='exchange',
            name='rate_limit',
            field=models.IntegerField(default=10000),
        ),
        migrations.AlterField(
            model_name='exchange',
            name='timeout',
            field=models.IntegerField(default=30000),
        ),
    ]
