# Generated by Django 3.0.6 on 2021-03-14 08:30

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('trading', '0082_auto_20210314_0829'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='account',
            name='margined',
        ),
    ]
