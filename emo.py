import requests
import json
import re
import os
import markdown
import pandas as pd
from requests_html import HTMLSession
from time import strftime, localtime, sleep


class Config:
    def __init__(self, urls, token, selected, path):
        self.urls = urls
        self.token = token
        self.selected = selected
        self.path = path
    # urls = ['https://exomol.com/data/molecules/NaO/23Na-16O/NaOUCMe/',
    #         'https://exomol.com/data/molecules/SiO/28Si-16O/SiOUVenIR/']

    # token = '87EdGUa0eTuaYZMkc4PFrZnlyQrTDc3Eq2LnQKgXhyHs2UfhjHygqC3nH5YL'

    # selected = ['Spectroscopic',
    #             'Definitions',
    #             'line list',
    #             'partition function',
    #             'opacity'
    #             ]
    # path = '/mnt/data/exomol/exomol3_data'


def collection(url: str, path_pre: str, selected):
    # data collection
    def get_data_general(item):
        """
        The function accepts an HTML response and returns a reformatted data information
        of a general description of ExoMol files
        """
        label = item.find(selector='h4')[0].text
        description = item.find(selector='p')[0].text
        references = []
        # ol and li are HTML/CSS keywords to locate the reference
        for element in item.find(selector='ol')[0].find(selector='li'):
            tmp_ref = element.text
            if 'link to article' in tmp_ref:
                tmp_ref = tmp_ref.replace(
                    'link to article', element.absolute_links.pop())
            elif '\nurl:' in tmp_ref:
                tmp_ref = tmp_ref.replace('\nurl:', '[') + ']'
            else:
                # print(tmp_ref)
                pass
            references.append(tmp_ref)

        files = []
        # li.list-group-item are HTML/CSS keywords to locate the file description and url
        for element in item.find(selector='li.list-group-item'):
            file_name = element.text.split('\n')[0].split(' ')[0]
            file_description = element.text.split('\n')[1]
            file_url = element.absolute_links.pop()
            files.append(
                {'file_name': file_name, 'description': file_description, 'url': file_url})

        return {label: {'upload': True, 'description': description, 'references': references, 'files': files}}

    def get_data(url, save=False):
        """
        The function accepts an url and returns data from different kids for data files
        """
        lst_general = {
            'line list',
            'partition function',
            'opacity',
            'cross section',
            'cooling function',
            'other States files',
            'heat capacity',
            'broadening coefficients',
            'program',
            'documentation',
            'super-line',
            'ExoCross'
        }
        s = HTMLSession()
        response = s.get(url).html.find(selector='div.well')

        res = {
            'url': url,
            'molecule': url.split("/")[-4],
            "isot": url.split("/")[-3],
            'dataset': url.split("/")[-2],
            'data': dict()
        }

        for item in response:
            # the definition file will be collected
            if 'Definitions file' in item.find(selector='h4')[0].text:
                for i in range(len(item.find(selector='h4'))):
                    label = item.find(selector='h4')[i].text
                    # print(label)
                    abs_links = item.find(
                        selector='div.list-group')[i].absolute_links.pop()
                    res['data'][label] = {'url': abs_links, 'upload': True}
                    info_def = HTMLSession().get(abs_links).text.split('\n')
                    for key in info_def:
                        if "YYYYMMDD" in key:
                            res["version"] = key.split()[0]
                            break
            # the spectrum file will be deleted
            elif 'Spectrum overview' in item.find(selector='h4')[0].text:
                label = item.find(selector='h4')[0].text
                res['data'][label] = {'url': item.find(
                    selector="img")[0].attrs['src'], 'upload': False}
            # other kinds of files will be collected using function get_data_general
            else:
                # for key in lst_general:
                #     if key in item.find(selector='h4')[0].text:
                res['data'].update(get_data_general(item))

        if save:
            with open('./arc/{iso}_{dataset}.json'.format(iso=res['isot'], dataset=res['dataset']), 'w') as f:
                json.dump(res, f)

        return res

    def repath(path: str, pre: str = path_pre):

        local_path = "/".join([pre] + path.split(".com/")[1::])
        local_path = local_path.replace("/db", '')

        return local_path

    def file_size(fs: int):
        label = {
            1: "bytes",
            2: "KB",
            3: "MB",
            4: "GB",
            5: "TB"
        }
        tag = 1
        while fs > 1:
            if fs < 1024:
                return " ".join([str(round(fs, 2)), label[tag]])
            else:
                tag += 1
                fs = fs / 1024

    def check_selected(key: str, selected=selected):

        if selected:
            pass
        else:
            selected = [
                'Spectroscopic',
                'Definitions',
                'line list',
                'partition function',
                'opacity'
            ]

        for kw in selected:
            if kw in key:
                return True
            else:
                pass

        return False

    def zenodo_data_prep_server(data: dict):
        # this function currently not in use
        for datum in list(data['data'].keys()):
            tmp = data['data'][datum]
            if "Spectroscopic" in datum:
                data['data'][datum]['upload'] = check_selected(datum)
            elif "Definitions" in datum:
                path = repath(tmp['url'])
                tmp['path'] = path
                try:
                    tmp['size'] = os.path.getsize(path)
                except:
                    tmp['size'] = 0
                    print(path)
                tmp['upload'] = check_selected(datum)
            elif 'Spectrum' in datum:
                del data['data'][datum]
            else:
                for item in data['data'][datum]['files']:
                    path = repath(tmp['url'])
                    item['path'] = path
                    item['size'] = os.path.getsize(path)
                    tmp += item['size']
                if not check_selected(datum):
                    data['data'][datum]['upload'] = False
                elif tmp / 1024 / 1024 / 1024 > 50:
                    data['data'][datum]['upload'] = 'over'
                else:
                    data['data'][datum]['upload'] = True

        return data

    def zenodo_data_prep_local(data: dict):
        for datum in list(data['data'].keys()):
            if "Spectroscopic" in datum:
                tmp = data['data'][datum]
                data['data'][datum]['upload'] = check_selected(datum)
            elif "Definitions" in datum:
                tmp = data['data'][datum]
                path = repath(tmp['url'])
                tmp['path'] = path
                tmp['upload'] = check_selected(datum)
            elif 'Spectrum' in datum:
                del data['data'][datum]
            else:
                for item in data['data'][datum]['files']:
                    path = repath(item['url'])
                    item['path'] = path
                if check_selected(datum, selected=selected):
                    data['data'][datum]['upload'] = True
                else:
                    data['data'][datum]['upload'] = False

        with open('./arc/{molecule}_{iso}_{dataset}.json'.format(
                molecule=data['molecule'],
                iso=data['isot'],
                dataset=data['dataset']),
                'w') as f:
            json.dump(data, f)

        return data

    def emo_md(data: dict, flag="full"):

        reference_base = """Tennyson, J., Yurchenko, S. N., Al-Refaie, A. F., Clark, V. H. J., Chubb, K. L., Conway,
        E. K., Dewan, A., Gorman, M. N., Hill, C., Lynas-Gray, A. E., Mellor, T., McKemmish, L. K., Owens, A.,
        Polyansky, O. L., Semenov, M., Somogyi, W., Tinetti, G., Upadhyay, A., Waldmann, I., Wang, Y., Wright, S.,
        Yurchenko, O. P., "The 2020 release of the ExoMol database: molecular line lists for exoplanet and other hot
        atmospheres", J. Quant. Spectrosc. Rad. Transf., 255, 107228 (2020). \[[
        https://doi.org/10.1016/j.jqsrt.2020.107228](https://doi.org/10.1016/j.jqsrt.2020.107228)\] """
        reference_ExoMol = "**References:**<br>\n> 1. {ref}".format(
            ref=reference_base)

        def italic(txt: str) -> str:
            return "*" + txt + "*"

        def bold(txt: str) -> str:
            return "**" + txt + "**"

        def insert_hyperlink(link: str) -> str:
            return "[{link}]({link})".format(link=link)

        def replace_link(ref: str) -> str:
            if 'http' in ref:
                for link in list(re.findall('\[http.*?\]', ref)):
                    ref = ref.replace(link, insert_hyperlink(link))
            return ref

        def insert_def(item):
            des = "\n\n# **Definitions file**\n"
            file_name = item['url'].split("/")[-1]
            des += "> " + bold(file_name) + "<br>\n\n"
            des += "> " + reference_ExoMol
            return des

        def insert_spectroscopic(item):
            des = "\n\n# **Spectroscopic Model**\n"
            des += "> " + insert_hyperlink(item['url']) + "<br>\n\n"
            return des

        def insert_general(data):
            des = str()
            if data['upload']:
                pass
            elif data['upload'] == 'over':
                des += msg_over
            elif not data['upload']:
                des += msg_no

            if flag == 'full':
                if "description" in data:
                    des += italic(data['description']) + '<br>\n'
                if 'files' in data:
                    for file in data['files']:
                        des += "\n> " + bold(file['file_name']) + '<br>'
                        des += file['description'] + '\n'
                if "references" in data:
                    des += "\n> **References:**<br>\n"
                    cnt = 1
                    for ref in data['references']:
                        ref = replace_link(ref)
                        des += "> " + str(cnt) + ". " + ref + '<br>\n'
                        cnt += 1
            elif flag == 'simple':
                if data['upload']:
                    if "description" in data:
                        des += italic(data['description']) + '<br>\n'
                    if 'files' in data:
                        for file in data['files']:
                            des += "\n> " + bold(file['file_name']) + '<br>'
                            des += file['description'] + '\n'
                    if "references" in data:
                        des += "\n> **References:**<br>\n"
                        cnt = 1
                        for ref in data['references']:
                            ref = replace_link(ref)
                            des += "> " + str(cnt) + ". " + ref + '<br>\n'
                            cnt += 1
                if data['upload'] == 'over':
                    if "references" in data:
                        des += "\n> **References:**<br>\n"
                        cnt = 1
                        for ref in data['references']:
                            ref = replace_link(ref)
                            des += "> " + str(cnt) + ". " + ref + '<br>\n'
                            cnt += 1
            return des

        def insert_file(tp, item):
            if "Definitions" in tp:
                return insert_def(item)
            elif "Spectroscopic" in tp:
                return insert_spectroscopic(item)
            else:
                return "\n\n# **{tp}**\n".format(tp=tp) + insert_general(item)

        molecule = data['molecule']
        isot = data['isot']
        db = data['dataset']
        # print("/".join([molecule, isot, db]))
        url = data['url']
        des_ini = 'The dataset is an archive of ExoMol page, {url}.<br>Please check the reference details according ' \
                  'to the following description or directly from the website.<br>\n'.format(
                      url=url)
        file_name = "{molecule}_{isot}_{db}".format(
            molecule=molecule, isot=isot, db=db)
        msg_simple = '**NB: The html description skips data which are not included in the current version for the ' \
                     'purpose of simplicity. Please check {file_name}.md for detailed information.**\n<br>'.format(
                         file_name=file_name)
        msg_over = "**NB: These data are not included in the current version on Zenodo because the data are over " \
                   "Zenodo upload cap, 50GB**<br>\n**Data can be accessed via:** {url}<br>\n".format(
                       url=url)
        msg_no = "**NB: These data are not included in current version on Zenodo**<br>\n**Data can be accessed via:**{url}<br>\n".format(
            url=url)
        des = des_ini
        if flag == 'simple':
            des += msg_simple
        for file in data['data']:
            des += insert_file(tp=file, item=data['data'][file])
        if flag == 'full':
            with open("./arc/{fn}.md".format(fn=file_name), 'w', encoding="utf-8") as fp:
                fp.write(des)
        elif flag == 'simple':
            with open("./arc/{fn}.html".format(fn=file_name), 'w', encoding="utf-8") as fp:
                fp.write(markdown.markdown(des))
        return des

    print('collecting data from {url}'.format(url=url))
    data = get_data(url)
    data = zenodo_data_prep_local(data)
    emo_md(data, flag='full')
    emo_md(data, flag='simple')

    return data


# data registration
def registration(data: dict, token: str):
    """
    data: archived json data
    token: Zenodo authorization token
    The main function of registration process.
    """

    def zenodo_ini(token: str):
        """
        token: str

        The function accepts token and creates an empty bucket for registration
        """

        headers = {"Content-Type": "application/json"}
        params = {'access_token': token}

        r = requests.post(
            'https://zenodo.org/api/deposit/depositions',
            params=params,
            json={},
            # Headers are not necessary here since "requests" automatically
            # adds "Content-Type: application/json", because we're using
            # the "json=" keyword argument
            # headers=headers,
            headers=headers
        )
        # r.status_code
        return r

    def zenodo_fill(*, deposit_id: str, metadata: dict, token: str):
        """
        The function uploads the metadata to fill keywords on Zenodo registration page
        """

        url = 'https://zenodo.org/api/deposit/depositions/%s' % deposit_id
        r = requests.put(
            url,
            params={'access_token': token},
            data=json.dumps(metadata),
            headers={"Content-Type": "application/json"}
        )

        # return message if upload successful or failed
        if r.status_code == 200:
            print('The {db} dataset for {isotope}, deposition id:{deposit_id} filling is successful'.format(
                db=db, isotope=isot, deposit_id=deposit_id))
        else:
            print('response code:{response}, something wrong filling'.format(
                response=r.status_code))

    def zenodo_upload(*, deposit_id: str, bucket_url: str, token: str):
        """
        bucket_url: the bucket id offered by Zenodo after creating a registration and for uploading files
        files: [file names]
        path_root: path of the database, usually default on ExoMol
        The function uploads relevant files via a files list to the Zenodo registration
        """
        file_description = "./arc/{molecule}_{isot}_{db}.md".format(
            molecule=molecule, isot=isot, db=db)
        with open("./" + file_description, "rb") as f_tmp:
            r = requests.put(
                "%s/%s" % (bucket_url, file_description),
                data=f_tmp,
                params={'access_token': token},
            )

        for file in data['data']:
            if data['data'][file]['upload'] == True:
                if 'Definitions' in file:
                    file_name = data['data'][file]['path'].split("/")[-1]
                    with open(data['data'][file]['path'], "rb") as f:
                        r = requests.put(
                            "%s/%s" % (bucket_url, file_name),
                            data=f,
                            params={'access_token': token},
                        )
                elif 'files' in data['data'][file]:
                    for item in data['data'][file]['files']:
                        with open(item['path'], "rb") as f:
                            r = requests.put(
                                "%s/%s" % (bucket_url, item['file_name']),
                                data=f,
                                params={'access_token': token},
                            )

        if r.status_code == 200:
            print('The {db} dataset for {isotope}, deposition id:{deposit_id} uploading is successful'.format(
                db=db, isotope=isot, deposit_id=deposit_id))
        else:
            print('response code:{response}, something wrong uploading'.format(
                response=r.status_code))

    def zenodo_metadata():
        """
        This function prepares metadata for the Zenodo reistration, including
        version matching
        creator matching
        grants matching
        keywords matching
        """

        def match_creators(references: list):
            """
            split the authors of references by first occurance
            the author information will be matched from creators_info
            which is collected from ORCID organization
            """
            creators_info = {
                "Tennyson, J.": {
                    "affiliation": "University College London",
                    "orcid": "0000-0002-4994-5238"
                },
                "Yurchenko, S. N.": {
                    "affiliation": "University College London",
                    "orcid": "0000-0001-9286-9501"
                },
                "Williams, H.": {"affiliation": "University College London"},
                "Victoria H. J. Clark": {
                    "affiliation": "University College London",
                    "orcid": "0000-0002-4384-2625"
                },
                "Leyland, P. C.": {"affiliation": "University College London"},
                "Chubb, K. L.": {
                    "affiliation": "SRON Netherlands Institute for Space Research",
                    "orcid": "0000-0002-4552-4559"
                },
                "Lodi, L.": {"affiliation": "University College London"},
                "Rocchetto, M.": {"affiliation": "University College London"},
                "Waldmann, I.": {"affiliation": "University College London"},
                "Barstow, J. K.": {"affiliation": "University College London"},
                "Al-Refaie, A. F": {"affiliation": "University College London"},
                "Molliere, P.": {"affiliation": "Max Planck Institute for Astronomy"}
            }
            creators = []
            tmp = []
            for ref in references:
                tmp = list(map(str.strip, ref.split("\"")[0].split(',')[:-1]))
                for item in [", ".join(item) for item in zip(tmp[0::2], tmp[1::2])]:
                    if item not in creators:
                        creators.append(item)
                # creators.update(set(", ".join(item) for item in zip(tmp[0::2], tmp[1::2])))
            creators_meta = list()
            for creator in creators:
                creators_meta.append({'name': creator})
                if creator in creators_info:
                    creators_meta[-1].update(creators_info[creator])
                else:
                    pass
            return creators_meta

        def match_keywords(data):
            # match keywords by the files included in dataset
            default_keywords = {'opacity': ['opacity', 'ExoMolOP'],
                                'line list': ['line list'],
                                'partition function': ['partition function']
                                }
            keywords = ["ExoMol"]
            for item in data['data']:
                for key in default_keywords:
                    if key in item:
                        keywords += default_keywords[key]
            return keywords

        def match_grants(data):
            # match keywords by the files included in dataset
            grants = []
            for key in data['data']:
                if 'opacity' in key:
                    grants += [{"id": "10.13039/501100000780::776403"}]
                elif 'line list' or 'partition function' in key:
                    grants += [
                        {"id": "10.13039/501100000780::883830"},
                        {"id": "267219"},
                        {"id": "10.13039/501100000690::ST/R000476/1"}
                    ]
            return grants

        def get_reference(data):
            ref = [
                """Tennyson, J., Yurchenko, S. N., Al-Refaie, A. F., Clark, V. H. J., Chubb, K. L., Conway, E. K., Dewan, A., Gorman, M. N., Hill, C., Lynas-Gray, A. E., Mellor, T., McKemmish, L. K., Owens, A., Polyansky, O. L., Semenov, M., Somogyi, W., Tinetti, G., Upadhyay, A., Waldmann, I., Wang, Y., Wright, S., Yurchenko, O. P., "The 2020 release of the ExoMol database: molecular line lists for exoplanet and other hot atmospheres", J. Quant. Spectrosc. Rad. Transf., 255, 107228 (2020). \[[https://doi.org/10.1016/j.jqsrt.2020.107228](https://doi.org/10.1016/j.jqsrt.2020.107228)\]"""]
            for file in data['data']:
                if "references" in data['data'][file]:
                    for item in data['data'][file]['references']:
                        if item not in ref:
                            ref = ref + [item]
            return ref

        references = get_reference(data)
        version = data['version']

        if version == '20192207':
            version = '20190722'

        creators = match_creators(references)
        publication_date = '-'.join([version[0:4], version[4:6], version[6::]])
        keywords = match_keywords(data)
        grants = match_grants(data)

        with open("./arc/{molecule}_{isot}_{db}.html".format(molecule=molecule, isot=isot, db=db), encoding='UTF-8') as f_tmp:
            description = f_tmp.read()

        # insert the prepared information of a metadata format
        metadata = {
            'metadata': {
                'title': 'The {db} dataset for {isotope}'.format(db=db, isotope=isot),
                'upload_type': 'dataset',
                'description': description,
                'creators': creators,
                'references': references,
                'license': "CC-BY-4.0",
                'publication_date': publication_date,
                'access_right': "open",
                'communities': [{'identifier': "exomol"}],
                'keywords': keywords,
                'version': version,
                'grants': grants
            }
        }

        return metadata

    db = data['dataset']
    isot = data['isot']
    molecule = data['molecule']
    r = zenodo_ini(token=token)
    bucket_url = r.json()["links"]["bucket"]
    deposit_id = r.json()['id']

    metadata = zenodo_metadata()
    zenodo_fill(deposit_id=deposit_id,
                metadata=metadata, token=token)
    zenodo_upload(deposit_id=deposit_id, bucket_url=bucket_url, token=token)


# supplementary functions
def url_parser(url):
    return "_".join(url.split("/")[-4:-1]) + ".json"


def rec_deposit(token, path_save='./'):
    """
    This function will collect the information of registered databases and associated DOI
    """
    response = requests.get('https://zenodo.org/api/deposit/depositions',
                            params={"size": 500, 'access_token': token})
    # response = requests.get('https://zenodo.org/api/records',
    #                     params={'access_token': token})
    res = response.json()
    # tmp = dict()
    # for item in res:
    #     tmp[item['title']] = {
    #         'doi': item['links']['doi'],
    #         'created date': item['created'],
    #         'version(publication_date)': item['metadata']['version']
    #     }

    # tmp = pd.DataFrame(tmp)
    tmp = pd.DataFrame(res)
    print(tmp)
    tmp.to_excel(path_save + strftime("%Y%m%d", localtime()) + '.xlsx', index=False)
    return


def ids_list_gen(token, ids='all'):
    # get ids for all unpublished registration form
    ids_list = list()
    # in case ids == 'all', a request will be sent to Zenodo and all unpublished
    # form will be deleted
    if ids == 'all':
        r = requests.get('https://zenodo.org/api/deposit/depositions',
                            params={'access_token': token, 'size': 200, 'status': 'draft'}).json()
        for item in r:
            ids_list.append(item['id'])
    elif isinstance(ids, str):
        ids_list = [ids]
    elif isinstance(ids, list) and len(ids) > 0:
        ids_list = ids
    return ids_list


def del_unpublished(token):
    """
    delete unpublished registration
    usually called when a new series of registration is started
    """
    def del_check(res):
        # check whether the deletion is successful
        if res.status_code == 204:
            print('del deposition id:%s success' % id)
        elif res.status_code == 404:
            print('Deposition file does not exist')
        elif res.status_code == 403:
            print('Deleting an already published deposition')
        else:
            print(res.status_code)

    if input('enter y to delete all drafts') == 'y':
        pass
    else:
        print('del aborted. \n exit')
        return None

    ids_list = ids_list_gen(token=token)
    # delete unpublished registration in dis_list
    for id in ids_list:
        r = requests.delete('https://zenodo.org/api/deposit/depositions/%s' % id,
                            params={'access_token': token})
        del_check(r)
        sleep(1)
    return None


def load_config(path):

    if not path:
        path = './config.json'
    else:
        pass

    with open(path) as f:
        config = Config(**json.load(f))

    return config


def publish(token):

    ids_list = ids_list_gen(token=token)

    if input('enter y to published all drafts') == 'y':
        pass
    else:
        print('del aborted. \n exit')
        return None
    for id in ids_list:
        res = requests.post(f'https://zenodo.org/api/deposit/depositions/{id}/actions/publish', params={'access_token': token})
        if res.status_code == 202:
            print(f'publish deposition id:{id} success')
        else:
            print(f'publish deposition id:{id} success error')
        sleep(1)
    
    return None




def emo_main(config):
    for url in config.urls:
        data = collection(url=url, path_pre=config.path, selected=config.selected)
        registration(data, token=config.token)

if __name__ == "__main__":

    config = load_config('./config.json')
    # emo_main(config)
    rec_deposit(token=config.token)
    # to check registration record
    # zenodo_rec_deposit(token=Config.token)
