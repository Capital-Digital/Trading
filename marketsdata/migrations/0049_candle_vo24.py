# Generated by Django 3.0.6 on 2020-08-12 08:02

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0048_auto_20200809_2222'),
    ]

    operations = [
        migrations.AddField(
            model_name='candle',
            name='vo24',
            field=models.FloatField(null=True),
        ),
    ]
