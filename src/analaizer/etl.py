# !sudo rm -rf /etc/localtime && sudo ln -s /usr/share/zoneinfo/America/Sao_Paulo /etc/localtime


def _read_foia_petitioners(filenames_or_buffers, **kwargs):
    return pd.concat(
        [
            pd.read_csv(
                filename_or_buffer,
                encoding="UTF-16",
                index_col="IdSolicitante",
                names=[
                    "IdSolicitante",
                    "TipoDemandante",
                    "DataNascimento",
                    "Genero",
                    "Escolaridade",
                    "Profissao",
                    "TipoPessoaJuridica",
                    "Pais",
                    "UF",
                    "Municipio",
                ],
                dtype={
                    "IdSolicitante": "Int64",
                    "TipoDemandante": "category",
                    "Genero": "category",
                    "Escolaridade": "category",
                    "Profissao": "category",
                    "TipoPessoaJuridica": "category",
                    "Pais": "category",
                    "UF": "category",
                    "Municipio": "category",
                },
                parse_dates=["DataNascimento"],
                **foia_readers_params,
            )
            for filename_or_buffer in filenames_or_buffers
        ]
    ).drop_duplicates()


def _read_foia_requests(filenames_or_buffers, **kwargs):
    requests = pd.DataFrame()

    # remove bad lines before reading
    for filename_or_buffer in filenames_or_buffers:
        with open(filename_or_buffer, "r", encoding="UTF-16") as source:
            # NOTE: reading one row at a time and concatenating it to the
            # previous rows is a terribly inneficient way of reading the
            # file. However, there are lines with missing columns, and pandas
            # does not provide a way of simply ignoring them (see
            # https://github.com/pandas-dev/pandas/issues/5686)
            i = 0
            for line in source.readlines():
                log.info(f"Reading {filename_or_buffer} (line {i})")
                if i > 3: break
                try:
                    requests = pd.concat([
                        requests,
                        pd.read_csv(
                            StringIO(line.rstrip("\n")),
                            index_col="IdPedido",
                            names=[
                                "IdPedido",
                                "ProtocoloPedido",
                                "Esfera",
                                "OrgaoDestinatario",
                                "Situacao",
                                "DataRegistro",
                                "ResumoSolicitacao",
                                "DetalhamentoSolicitacao",
                                "PrazoAtendimento",
                                # "FoiProrrogado",
                                # "FoiReencaminhado",
                                "FormaResposta",
                                "OrigemSolicitacao",
                                "IdSolicitante",
                                "AssuntoPedido",
                                "SubAssuntoPedido",
                                "DataResposta",
                                "Resposta",
                                "Decisao",
                                "EspecificacaoDecisao",
                            ],
                            dtype={
                                "IdPedido": "Int64",
                                "ProtocoloPedido": str,
                                "Esfera": "category",
                                "OrgaoDestinatario": "category",
                                "Situacao": "category",
                                "ResumoSolicitacao": str,
                                "DetalhamentoSolicitacao": str,
                                "FoiProrrogado": bool,
                                "FoiReencaminhado": bool,
                                "FormaResposta": "category",
                                "OrigemSolicitacao": "category",
                                "IdSolicitante": "Int64",
                                "AssuntoPedido": str,
                                "SubAssuntoPedido": str,
                                "Resposta": str,
                                "Decisao": "category",
                                "EspecificacaoDecisao": "category",
                            },
                            parse_dates=[
                                "DataRegistro",
                                # "DataResposta",
                                # "PrazoAtendimento",
                            ],
                            # true_values=["Sim"],
                            # false_values=["Não"],
                            usecols=[
                                "IdPedido",
                                "ProtocoloPedido",
                                "OrgaoDestinatario",
                                "Situacao",
                                "DataRegistro",
                                "AssuntoPedido",
                                "Decisao",
                            ],
                            nrows=10,
                            **foia_readers_params,
                            **kwargs,
                        ),
                    ])
                except (ValueError, TypeError, ParserError):
                    pass
                finally:
                    i += 1
    return requests.drop_duplicates()


def _read_foia_appeals(filenames_or_buffers, **kwargs):
    instances_order = (
        "Pedido de Revisão",
        "Primeira Instância",
        "Segunda Instância",
        "CGU",
        "CMRI",
    )
    instances_short_names = {
        "Pedido de Revisão": "Revisao",
        "Primeira Instância": "1aInstancia",
        "Segunda Instância": "2aInstancia",
        "CGU": "CGU",
        "CMRI": "CMRI",
    }
    col_types = {
        "IdRecurso": "Int64",
        "IdRecursoPrecedente": "Int64",
        "DescRecurso": str,
        "IdPedido": "Int64",
        "IdSolicitante": "Int64",
        "ProtocoloPedido": "Int64",
        "OrgaoDestinatario": "category",
        "Instancia": "category",
        "Situacao": "category",
        "DataRegistro": "datetime64",
        "PrazoAtendimento": "datetime64",
        "OrigemSolicitacao": "category",
        "DataResposta": "datetime64",
        "TipoRecurso": "category",
        "RespostaRecurso": str,
        "TipoResposta": "category",
    }
    if not isinstance(filenames_or_buffers, Iterable):
        filenames_or_buffers = [filenames_or_buffers]
    appeals_readers = chain.from_iterable([
        pd.read_csv(
            filename_or_buffer,
            encoding="UTF-16",
            names=list(col_types.keys()),
            dtype={
                k: v for k, v in col_types.items() if v != "datetime64"
            },
            # parse_dates=[
            #     k for k, v in col_types.items() if v == "datetime64"
            # ],
            usecols=[
                "IdPedido",
                "ProtocoloPedido",
                "Instancia",
                "Situacao",
                "TipoResposta",
            ],
            chunksize=1,
            # memory_map=True,
            # engine="python",
            quoting=csv.QUOTE_NONE,
            **foia_readers_params,
            **kwargs,
        )
        for filename_or_buffer in filenames_or_buffers
    ])

    appeals_long = pd.DataFrame()
    for chunk in appeals_readers:
        try:
            appeals_long = pd.concat([appeals_long, chunk], ignore_index=True)
        except (ValueError, TypeError, ParserError):
            pass

    appeals_long = (
        appeals_long
        .sort_values(
            "Instancia",
            key=lambda s: s.apply(lambda i: instances_order.index(i))
        )
        .assign(
            Instancia=lambda _: _["Instancia"].map(instances_short_names)
        )
        # .drop(columns=["IdRecursoPrecedente"])
    )

    last_appeal = appeals_long.groupby("IdPedido").last().reset_index()
    last_appeal["Instancia"] = "Final"
    appeals_wide = (
        appeals_long
        .append(last_appeal, ignore_index=True)
        .pivot(
            index=[
                "IdPedido",
                "ProtocoloPedido",
                # "IdSolicitante",
            ],
            columns=["Instancia"],
        )
        .sort_index()
    )
    appeals_wide.columns = [
        "".join(col).strip() for col in appeals_wide.columns.values
    ]
    return appeals_wide.drop_duplicates()


_foia_readers = {
    "Solicitantes": _read_foia_petitioners,
    "Pedidos": _read_foia_requests,
    "Recursos": _read_foia_appeals,
}


def _join_foia_datasets(
    # petitioners: DataFrame,
    requests: DataFrame,
    appeals: DataFrame,
) -> DataFrame:
    return (
        petitioners
        # .merge(
        #     requests,
        #     left_index=True,
        #     right_on="IdSolicitante",
        #     how="right",
        # )
        .reset_index()
        .merge(
            appeals,
            left_on=[
                "IdPedido",
                "ProtocoloPedido",
                # "IdSolicitante",
            ],
            right_index=True,
            how="left",
        )
    )


def get_govbr_foia(
    start_year: int = 2015,
    end_year: int = NOW.year,
    refresh_cache: bool = False,
) -> DataFrame:
    """Downloads data on FOIA requests to Brazilian Federal Government.
    
    This function returns a pandas `DataFrame` encompassing data on Freedom
    of Information Act (FOIA) petitioners, requests and appeals directed to
    Brazilian Federal Government in a given historical interval.

    Arguments:
        start_year: The first year of the historical interval whose FOIA
            requests are to be returned. Must be equal or greater than 2015.
        end_year: The last year of the historical interval whose FOIA
            requests are to be returned. Must be equal or greater than
            `start_year`.
    
    Returns:
        A `pandas.dataframe.DataFrame` object with data on FOIA petitioners,
        requests and appeals.
    
    Raises:
        ValueError: if `start_year` argument is before 2015, of if `end_year`
            is before `end_year`.
    """

    if start_year < 2015:
        raise ValueError(
            "Argument `start_year` must be equal or greater than 2015."
        )
    if end_year < start_year:
        raise ValueError(
            "Argument `end_year` must be equal or greater than `start_year`."
        )

    results = []
    for dataset_type in [
        # "Solicitantes",
        "Pedidos",
        "Recursos",
    ]:
        dataset = pd.DataFrame()
        file_paths = []
        for year in range(start_year, end_year):
            available_files = sorted(
                IN_DATA_DIR.glob(f"????????_{dataset_type}_csv_{year}.csv")
            )
            if available_files and not refresh_cache:
                # use latest file
                # TODO: update only if year is the current year 
                file_paths.append(str(available_files[-1]))
            else:
                # download and unzip files
                url = (
                    "https://dadosabertos-download.cgu.gov.br/FalaBR"
                    f"/Arquivos_FalaBR_Filtrado/Arquivos_csv_{year}.zip"
                )
                http_response = urlopen(url)
                zipfile = ZipFile(BytesIO(http_response.read()))
                zipfile.extractall(path=IN_DATA_DIR)
                file_paths.append(Path(
                    IN_DATA_DIR, f"{NOW:%Y%m%d}_{dataset_type}_csv_{year}.csv"
                ))

        dataset = _foia_readers[dataset_type](file_paths)
        results.append(dataset)
    return _join_foia_datasets(*results)


lai_requests = get_govbr_foia()