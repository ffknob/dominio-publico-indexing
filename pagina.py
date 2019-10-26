from datetime import datetime
from elasticsearch_dsl import Document, Text, Date

ELASTICSEARCH_INDEX = 'dominio-publico'

class Pagina(Document):
    autor = Text()
    titulo = Text()
    categoria = Text()
    idioma = Text()
    instituicao = Text()
    acessos = int()
    pagina = int()
    base64 = Text()
    texto = Text()
    created_at = Date()

    class Index:
        name = ELASTICSEARCH_INDEX

    def save(self, ** kwargs):
        self.created_at = datetime.now()
        return super().save(** kwargs)
