# Generated by Django 3.0.6 on 2020-07-14 13:42

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0028_market_updated'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='market',
            name='future',
        ),
        migrations.RemoveField(
            model_name='market',
            name='spot',
        ),
        migrations.RemoveField(
            model_name='market',
            name='swap',
        ),
    ]
