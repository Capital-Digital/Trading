# Generated by Django 3.0.6 on 2021-03-06 15:29

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0098_auto_20210305_1810'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='currency',
            name='market_type',
        ),
        migrations.RemoveField(
            model_name='exchange',
            name='market_types',
        ),
        migrations.RemoveField(
            model_name='market',
            name='type',
        ),
        migrations.DeleteModel(
            name='MarketType',
        ),
    ]
