# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models

# Create your models here.

class User_info(models.Model):
    userid = models.CharField(max_length=70, blank=False, default='')
    email = models.EmailField(max_length=200,blank=False, default='')
    passwd = models.CharField(max_length=70, blank=False, default='')

    def __str__(self) -> str:
        return self.userid