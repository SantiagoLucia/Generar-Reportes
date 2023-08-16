from datetime import datetime

from sqlalchemy import create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.types import DateTime, Integer, String
import pandas as pd

class Base(DeclarativeBase):
    pass


class Documento(Base):
    __tablename__ = "gedo_documento"
    
    id: Mapped[int] = mapped_column(Integer(), primary_key=True, nullable=False)
    numero: Mapped[str] = mapped_column(String(4000))
    reparticion: Mapped[str] = mapped_column(String(4000))
    fechacreacion: Mapped[datetime] = mapped_column(DateTime())
    tipo: Mapped[int] = mapped_column(Integer())
        
    def __repr__(self) -> str:
        return (
            f"Documento(id={self.id!r}, "
            f"numero={self.numero!r}"
        )


class TipoDocumento(Base):
    __tablename__ = "gedo_tipodocumento"
    
    id: Mapped[int] = mapped_column(Integer(), primary_key=True, nullable=False)
    acronimo: Mapped[str] = mapped_column(String(4000))
            
    def __repr__(self) -> str:
        return (
            f"TipoDocumento(id={self.id!r}, "
            f"acronimo={self.acronimo!r}"
        )        
        
def main() -> None:
    id_documento = int(
        input("Ingrese el id del documento: ")
    )

    engine = create_engine(rf"oracle+cx_oracle://SLPRIVITERA:SP718GM33@rodb.gdeba.gba.gob.ar:10521/?service_name=dpmamjgm.gdeba.gba.gob.ar")
    # session = Session(engine)
    stmt = (
        select(
            Documento.id,
            Documento.numero,
            Documento.fechacreacion,
            TipoDocumento.acronimo
        )
        .join(TipoDocumento, Documento.tipo == TipoDocumento.id)
        .where(Documento.id == id_documento)
    )

    # for doc in session.execute(stmt):
    #         print(doc)

    with engine.connect() as conn:
        data = pd.read_sql(stmt, con=conn)
        print(data)

if __name__ == "__main__":
    main()