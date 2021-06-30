# Generated by Django 3.0.6 on 2020-07-28 15:12

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0043_auto_20200726_2005'),
        ('trading', '0018_account_markets'),
    ]

    operations = [
        migrations.AlterField(
            model_name='account',
            name='markets',
            field=models.ManyToManyField(blank=True, related_name='account', to='marketsdata.Market'),
        ),
    ]
