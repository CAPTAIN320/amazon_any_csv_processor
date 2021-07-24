import glob
from dominate import document
from dominate.tags import *

html_files = glob.glob('html/*.html')


with document(title='AMAZON CATEGORIES') as doc:
    h1('Amazon Categories')
    for path in html_files:
        h3(li(a(path[5:-5].title(), href='%s' % path)))


with open('index.html', 'w') as f:
    f.write(doc.render())

