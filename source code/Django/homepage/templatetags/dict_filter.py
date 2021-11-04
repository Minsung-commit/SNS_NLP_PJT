from django.template.defaulttags import register

@register.filter('get_dict')
def get_dict(dictionary, key):
    if key in dictionary:
        return dictionary.get(key)