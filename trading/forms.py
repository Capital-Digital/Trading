from django.contrib.auth.forms import UserCreationForm
from django.forms import *
from marketsdata.models import Exchange
from trading.models import Account


class CustomUserCreationForm(UserCreationForm):
    class Meta(UserCreationForm.Meta):
        fields = UserCreationForm.Meta.fields + ("email",)


class AccountForm(ModelForm):
    class Meta:
        model = Account
        fields = ['name', 'exchange', 'strategy', 'api_key', 'password', 'active', 'limit_order', 'email']


class AccountDeleteForm(ModelForm):
    class Meta:
        model = Account
        fields = []