# Generated by Django 3.0.6 on 2021-05-30 11:29

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('trading', '0120_auto_20210530_1128'),
    ]

    operations = [
        migrations.AlterField(
            model_name='position',
            name='value',
            field=models.FloatField(max_length=100, null=True),
        ),
    ]
