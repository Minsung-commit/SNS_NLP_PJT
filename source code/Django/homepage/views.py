# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import json
from os import name

from pymongo import MongoClient
from django.shortcuts import render
from django.http import HttpResponseRedirect
from django.urls import reverse
from homepage.models import User_info
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login
from django.contrib import auth

# from homepage.MongoDbManager import MongoDbManager
from datetime import datetime
import pandas as pd
from homepage.Api import Json, recommandation
from homepage.MongoDbManager import MongoDbManager_insta
#from .forms import SignUpForm

# 0 = '혼자만의 숙소' 2 = '데이트하기 좋은 숙소' 3 = '특별한 체험이 있는 숙소' 4 = '고즈넉한 숙소' 5 = '감각있는 숙소'
# 6 = '아름다운 풍경이 가득한 숙소' 7 = '놀기 좋은 숙소' 8 = '편안한 분위기의 숙소' 9 = '하늘과 바다가 가득한 숙소'


filter = {'zero' : '오롯이 나를 위해 보내는 하루' , 'two':'너와 나, 우리 둘만의 하루', 'three':'당신의 하루를 특별하게','four':'고즈넉한 사색의 공간','five':'우리들만의 파티 플레이스',\
    'six':'자연 그대로를 품다','seven':'자연에서의 놀이터','eight':'따듯하고 포근한 공간을 그리며','nine':'하늘과 바다가 가득 밀려드는'}
# Create your views here.
def home(requests):
    return render(requests, 'homepage/index.html')

def register(requests):
    return render(requests, 'homepage/register.html')

def regcon(requests):
    userid = requests.POST['userid']
    email = requests.POST['emailid']
    passwd = requests.POST['userpw']

    qs = User.objects.create_user(userid, email, passwd)
    qs.save()

    return HttpResponseRedirect(reverse('homepage:register'))

def logcon(requests):
    if requests.method == 'POST':
        userid = requests.POST['userid']
        passwd = requests.POST['userpw']
        result = authenticate(username = userid, password = passwd)
        user=User()


        if result is not None:
            # login(result)
            return HttpResponseRedirect(reverse('homepage:choice'))
        else:
            #return HttpResponseRedirect(reverse('homepage:relogin'))
            return render(requests, 'homepage/relogin.html')
            
def login(requests):
    return render(requests, 'homepage/login.html')

def choice(requests):
    return render(requests, 'homepage/choice.html')

def totalstay(requests):
    print(dict(requests.POST.items()))
    if requests.method == 'POST':
        if requests.POST.get('local') == 0:
            print(1)
        if requests.POST.get('tag') and requests.POST.get('local') != '0':
            tag = requests.POST.get('tag')
            local  = requests.POST.get('local')
            print(tag, local)
            context = Json.tag_local_find_name(tag, local)

        elif requests.POST.get('tag'):
            tag  = requests.POST.get('tag')
            print(tag)
            context = Json.tag_find_name(tag)

        elif requests.POST.get('local'):
            local  = requests.POST.get('local')
            print(local)
            context = Json.local_find_name(local)

        
        
    # elif requests.method == 'get':
    #     requests.GET.get()
    else:
        context = Json.all_info()
    return render(requests, 'homepage/totalstay.html', context)


def popular(requests):
    context = Json.most_like()
    return render(requests, 'homepage/popular.html', context)


def showTheme(requests):
    context = filter
    return render(requests, 'homepage/theme.html', context)

def stayFilter(requests, theme):
    nowtime = datetime.today().strftime("%Y-%m-%d")
    co_if = Json.corona_info(nowtime)
    insta_if = Json.insta_info(theme)
    co_if.update(insta_if)
    co_if['filter'] = theme
    return render(requests, 'homepage/stayfilter.html', co_if)

def stayDetail(requests, name): 
    name_info = {}
    context = recommandation.insta_REC(name)
    context['b10'] = name
    output = Json.name_info(context)
    return render(requests, 'homepage/staydetail.html', output)

