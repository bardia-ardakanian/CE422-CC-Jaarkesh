import json
from bson import json_util, ObjectId
from django.shortcuts import render
from django.http import HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from app.models import State, Response
from app.utils import submit, get_promotion_by_id


# Create your views here.
def hello_world(request):
    return HttpResponse(Response(0, None, "Hi Mom!").json())


@csrf_exempt
def submit_promotion(request):
    pid, mid = submit(
        request.GET.get('description'),
        request.GET.get('email'),
        request.GET.get('file_path'),
    )

    body = 'Promotion Submitted [pid:{0}]'.format(pid)
    return HttpResponse(Response(0, None, body).json())


def get_promotion(request):
    promotion = get_promotion_by_id(request.GET.get('_pid'))
    state = State(promotion['state'])

    body = None
    if state == State.PROCESSING:
        body = 'Promotion [{0}] is in processing Queue'.format(request.GET.get('_pid'))
    elif state == State.ACCEPTED:
        body = f"""Promotion [{promotion['_id']}] has been ACCPETED. 
        {json.loads(json_util.dumps(promotion, indent=2))}"""
        # .format(promotion['_id'], promotion['description'], promotion['email'], State(promotion['state']).name,
        #         promotion['category'])
    elif state == State.REJECTED:
        body = 'Promotion [{0}] has been REJECTED.'.format(request.GET.get('_pid'))
    else:
        body = 'There has been a problem in our service. Please try again later!'

    return HttpResponse(Response(0, None, body).json())
