# Generated by Django 3.0.6 on 2020-07-22 16:01

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('trading', '0002_auto_20200722_1600'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='order',
            name='account',
        ),
    ]
