# Generated by Django 3.0.6 on 2021-05-30 11:53

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0150_market_funding_rate'),
    ]

    operations = [
        migrations.AlterField(
            model_name='market',
            name='contract_value',
            field=models.IntegerField(blank=True, null=True),
        ),
    ]
