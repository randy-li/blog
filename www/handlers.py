#!/usr/bin/env python3
# -*- coding: utf-8 -*-


__author__ = 'Randy Li'


' url handlers'


import re, time, json, logging, hashlib, base64, asyncio

from coroweb import get, post
from models import User, Comment, Blog, next_id


@get('/')
async def index(request):
    summary = 'Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore ' \
              'et dolore magna aliqua.'
    blogs = []
    return {
        '__template__': 'blogs.html',
        'users': blogs
    }