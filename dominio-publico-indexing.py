# -*- coding: utf-8 -*-
import sys
import os
from argparse import ArgumentParser
import logging
from PyPDF2 import PdfFileReader, PdfFileWriter
import base64
from elasticsearch_dsl import connections
from pagina import Pagina

LOG_FILE = 'dominio-publico.log'
LOG_LEVEL = logging.INFO
logger = None
args = None

def set_arguments_parser():
    """Parâmetros de execução"""
    parser = ArgumentParser(description=u'Indexação de documentos PDF do portal Domínio Público no Elasticsearch.')
    parser.add_argument('file', help=u'Path para o arquivo contendo o documento PDF')

    return parser

def set_logging():
    """Configuração de logging"""
    logger = logging.getLogger('dominio-publico')
    logger.setLevel(LOG_LEVEL)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    logger_fh = logging.FileHandler(LOG_FILE)
    logger_fh.setLevel(LOG_LEVEL)
    logger_fh.setFormatter(formatter)
    logger.addHandler(logger_fh)

    logger_ch = logging.StreamHandler()
    logger_ch.setLevel(LOG_LEVEL)
    logger_ch.setFormatter(formatter)
    logger.addHandler(logger_ch)

    return logger

def create_elasticsearch_connection(logger):
    """Cria conexão com o Elasticsearch """
    connections.create_connection(hosts=['localhost'], timeout=20)

def index_pdf(logger, file_path, metadata):
    """Indexa documento PDF no Elasticsearch """

    logger.debug("Metadados: {} ".format(metadata))

    fname = os.path.splitext(os.path.basename(file_path))[0]
    pdf = PdfFileReader(file_path)
    num_pages = pdf.getNumPages()
    logger.debug("PDF possui {}".format(num_pages))

    for num_page in range(pdf.getNumPages()):
        logger.info("Página {}/{}".format(num_page+1, num_pages))

        pdf_writer = PdfFileWriter()
        pdf_writer.addPage(pdf.getPage(num_page))

        page_filename = '/tmp/{}-{}.pdf'.format(fname, num_page+1)

        logger.info("Criando arquivo {}".format(page_filename))

        with open(page_filename, 'wb') as page:
            pdf_writer.write(page)

        with open(page_filename, 'rb') as page_file:
            page_content_base64_encoded = base64.b64encode(page_file.read())
            page_content_base64_str = str(page_content_base64_encoded, 'utf-8')

            document = Pagina(
                autor=metadata['autor'],
                titulo=metadata['titulo'],
                categoria=metadata['categoria'],
                idioma=metadata['idioma'],
                instituicao=metadata['instituicao'],
                acessos=metadata['acessos'],
                pagina=num_page+1,
                base64=page_content_base64_str
            )

            logger.info("Indexando página {}...".format(num_page+1))

            document.save(pipeline='dominio-publico-extracao-pdf')

            logger.info("Removendo arquivo {}".format(page_filename))

            os.remove(page_filename)

def main():
    parser = set_arguments_parser()
    args = parser.parse_args()

    if not args.file:
        parser.print_help(sys.stderr)
        sys.exit(1)

    logger = set_logging()

    logger.info("Indexando documento {}...".format(args.file))

    logger.info("Conectando no Elasticsearch...")

    create_elasticsearch_connection(logger)

    metadata = {
            "autor": "Machado de Assis",
            "titulo": "Dom Casmurro",
            "categoria": "Literatura",
            "idioma": "Português",
            "instituicao": "[bv] Biblioteca Virtual do Estudante Brasileiro / USP",
            "acessos": 262596
    }

    index_pdf(logger, args.file, metadata)

if __name__ == '__main__':
    main()


