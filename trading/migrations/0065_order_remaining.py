# Generated by Django 3.0.6 on 2020-10-19 13:22

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('trading', '0064_auto_20201019_1321'),
    ]

    operations = [
        migrations.AddField(
            model_name='order',
            name='remaining',
            field=models.CharField(max_length=10, null=True),
        ),
    ]
