# Generated by Django 3.0.6 on 2021-03-13 17:33

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('trading', '0074_auto_20210313_1613'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='fund',
            name='currency',
        ),
        migrations.RemoveField(
            model_name='fund',
            name='response',
        ),
        migrations.RemoveField(
            model_name='fund',
            name='type',
        ),
    ]
