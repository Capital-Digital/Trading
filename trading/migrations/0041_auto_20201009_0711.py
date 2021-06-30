# Generated by Django 3.0.6 on 2020-10-09 07:11

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('trading', '0040_auto_20201009_0652'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='position',
            name='position_mode',
        ),
        migrations.AddField(
            model_name='account',
            name='position_mode',
            field=models.CharField(blank=True, choices=[('dual', 'dual'), ('hedge', 'hedge')], max_length=20),
        ),
    ]
