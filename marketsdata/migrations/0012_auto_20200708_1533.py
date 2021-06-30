# Generated by Django 3.0.6 on 2020-07-08 15:33

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0011_exchange_trade_base_currency'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='exchange',
            name='trade_base_currency',
        ),
        migrations.AddField(
            model_name='exchange',
            name='trade',
            field=models.CharField(blank=True, choices=[('base', 'base'), ('quote', 'quote')], default='base', max_length=12, null=True),
        ),
    ]
