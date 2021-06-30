# Generated by Django 3.0.6 on 2020-09-28 20:13

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('marketsdata', '0053_auto_20200921_0836'),
        ('trading', '0028_auto_20200928_2009'),
    ]

    operations = [
        migrations.AlterField(
            model_name='account',
            name='exchange',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='account', to='marketsdata.Exchange'),
        ),
        migrations.AlterField(
            model_name='fund',
            name='currency',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='fund', to='marketsdata.Currency'),
        ),
        migrations.AlterField(
            model_name='fund',
            name='exchange',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='fund', to='marketsdata.Exchange'),
        ),
        migrations.AlterField(
            model_name='order',
            name='account',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.CASCADE, related_name='order', to='trading.Account'),
        ),
        migrations.AlterField(
            model_name='order',
            name='market',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='order', to='marketsdata.Market'),
        ),
        migrations.AlterField(
            model_name='position',
            name='exchange',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='position', to='marketsdata.Exchange'),
        ),
        migrations.AlterField(
            model_name='position',
            name='market',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='position', to='marketsdata.Market'),
        ),
    ]
