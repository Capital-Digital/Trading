# Generated by Django 3.0.6 on 2021-03-06 15:54

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0105_auto_20210306_1553'),
    ]

    operations = [
        migrations.RenameField(
            model_name='exchange',
            old_name='ccxt',
            new_name='ccxt_id',
        ),
    ]
