# Generated by Django 3.0.6 on 2021-03-06 15:46

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0101_auto_20210306_1546'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='market',
            name='collateral_currency',
        ),
    ]
