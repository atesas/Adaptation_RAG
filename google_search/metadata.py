from utils import parse_date_generic

def extract_metadata(item):
    pagemap = item.get('pagemap', {})
    metatags = (pagemap.get('metatags') or [{}])[0]
    def first(*keys):
        for key in keys:
            if metatags.get(key): return metatags[key]
        return ''
    return {
        'title': item.get('title', ''),
        'snippet': item.get('snippet', ''),
        'link': item.get('link', ''),
        'display_link': item.get('displayLink', ''),
        'mime_type': item.get('mime', ''),
        'file_format': item.get('fileFormat', ''),
        'creation_date': parse_date_generic(metatags.get('creationdate', '')),
        'modified_date': parse_date_generic(
            metatags.get('moddate', '') or metatags.get('article:modified_time', '') or metatags.get('og:updated_time', '')
        ),
        'published_date': parse_date_generic(metatags.get('article:published_time', '')),
        'creator': metatags.get('creator', ''),
        'producer': metatags.get('producer', ''),
        'author': first('author', 'article:author', 'creator:author', 'by')
    }
