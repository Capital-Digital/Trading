# Generated by Django 3.0.6 on 2021-03-14 08:23

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('trading', '0080_auto_20210314_0823'),
    ]

    operations = [
        migrations.RenameField(
            model_name='account',
            old_name='contract_margin',
            new_name='margined',
        ),
    ]
