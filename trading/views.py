from django.http import HttpResponse, HttpResponseRedirect
from django.contrib.auth.models import User
from django.contrib import messages
from django.contrib.auth import login, decorators
from django.shortcuts import redirect, render, get_object_or_404
from django.urls import reverse
from trading.forms import CustomUserCreationForm, AccountForm, AccountDeleteForm
from trading.models import Strategy, Account, Allocation
from pprint import pprint
import datetime


def index(request):
    return render(request, "users/dashboard.html")
    # return HttpResponse()


def register(request):
    if request.method == "GET":
        return render(
            request, "register.html",
            {"form": CustomUserCreationForm}
        )
    elif request.method == "POST":
        form = CustomUserCreationForm(request.POST)
        if form.is_valid():
            user = form.save(commit=False)
            user.backend = "django.contrib.auth.backends.ModelBackend"
            user.save()
            login(request, user)
            return redirect(reverse("dashboard"))


def dashboard(request):
    data = {
        "strategies": Strategy.objects.all(),
        "accounts": Account.objects.all()
    }

    return render(request, "dashboard.html", data)


# def strategies(request):
#
#     data = {
#         "strategies": Strategy.objects.all()
#     }
#
#     return render(request, "strategies.html", data)


def accounts(request):
    data = {
        "accounts": Account.objects.filter(user=request.user)
    }

    return render(request, "accounts.html", data)


def allocations(request, strategy_id):
    data = {
        "allocations": Allocation.objects.filter(strategy__id=strategy_id).order_by('-dt')[:48],
        "strategy": Strategy.objects.get(id=strategy_id)
    }

    return render(request, "allocations.html", data)


@decorators.login_required
def account(request, account_id):
    data = {
        "account": Account.objects.get(id=account_id)
    }

    return render(request, "account/account.html", data)


def account_create(request):
    if request.method == 'POST':
        form = AccountForm(request.POST)
        if form.is_valid():
            form.instance.user = request.user
            print('user', form.instance.user)
            form.save()
            return redirect('accounts')
    else:
        form = AccountForm()
    return render(request,
                  'account/account_create.html',
                  {
                      'form': form
                  })


def account_edit(request, pk=None):
    account = get_object_or_404(Account, pk=pk)
    if request.method == "POST":
        form = AccountForm(request.POST, instance=account)
        if form.is_valid():
            form.save()
            return redirect('account')
    else:
        form = AccountForm(instance=account)

    return render(request,
                  'account/account_edit.html',
                  {
                      'form': form,
                      'account': account
                  })


def account_delete(request, pk=None):
    account = get_object_or_404(Account, pk=pk)
    if request.method == "POST":
        form = AccountDeleteForm(request.POST, instance=account)
        if form.is_valid():
            if request.user == account.user:
                account.delete()
                return redirect('account')
            else:
                return redirect('account')
    else:
        form = AccountDeleteForm(instance=account)

    return render(request, 'account/account_delete.html',
                  {
                      'form': form,
                      'account': account,
                  })


def see_request(request):
    text = f"""
        Some attributes of the HttpRequest object:

        scheme: {request.scheme}
        path:   {request.path}
        method: {request.method}
        GET:    {request.GET}
        user:   {request.user}
    """

    return HttpResponse(text, content_type="text/plain")


def user_info(request):
    text = f"""
        Selected HttpRequest.user attributes:

        username:     {request.user.username}
        is_anonymous: {request.user.is_anonymous}
        is_staff:     {request.user.is_staff}
        is_superuser: {request.user.is_superuser}
        is_active:    {request.user.is_active}
        permission add:   {request.user.has_perm('trading.add_account')}
        permission view:   {request.user.has_perm('trading.view_account')}
    """

    return HttpResponse(text, content_type="text/plain")


@decorators.login_required
def private_place(request):
    return HttpResponse("Shhh, members only!", content_type="text/plain")


@decorators.user_passes_test(lambda user: user.is_staff)
def staff_place(request):
    return HttpResponse("Employees must wash hands", content_type="text/plain")


@decorators.login_required
def add_messages(request):
    username = request.user.username
    messages.add_message(request, messages.INFO, f"Hello {username}")
    messages.add_message(request, messages.WARNING, "DANGER WILL ROBINSON")

    return HttpResponse("Messages added", content_type="text/plain")