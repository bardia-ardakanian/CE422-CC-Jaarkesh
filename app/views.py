import json
from bson import json_util, ObjectId
from django.shortcuts import render
from django.http import HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from app.models import State, Response
from app.utils import submit, get_promotion_by_id, handle_uploaded_file, generate_name


# Create your views here.
def hello_world(request):
    return HttpResponse(Response(0, None, "Hi Mom!").json())


@csrf_exempt
def submit_promotion(request):
    pid, mid = submit(
        request.POST.get('description'),
        request.POST.get('email'),
        handle_uploaded_file(request.FILES['image'], generate_name(4)),
    )

    body = 'Promotion Submitted [pid:{0}]'.format(pid)
    return HttpResponse(Response(0, None, body).json())


def get_promotion(request):
    promotion = get_promotion_by_id(request.GET.get('_pid'))
    state = State(promotion['state'])

    if state == State.PROCESSING:
        body = 'Promotion [{0}] is in processing Queue'.format(request.GET.get('_pid'))
    elif state == State.ACCEPTED:
        info = {
            'Message': 'Promotion [{0}] has been ACCEPTED'.format(request.GET.get('_pid')),
            'Promotion': promotion,
        }
        body = json.loads(json_util.dumps(info, indent=2))
    elif state == State.REJECTED:
        body = 'Promotion [{0}] has been REJECTED.'.format(request.GET.get('_pid'))
    else:
        body = 'There has been a problem in our service. Please try again later!'

    return HttpResponse(Response(0, None, body).json())
