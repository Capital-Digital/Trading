# Generated by Django 3.0.6 on 2021-03-12 07:55

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0131_market_excluded'),
    ]

    operations = [
        migrations.RenameField(
            model_name='market',
            old_name='type_ccxt',
            new_name='ccxt_type',
        ),
    ]
