from gravitino import GravitinoClient


# def test_foobar():
gravitino = GravitinoClient("http://localhost")
# print(gravitino.service.get_partitions("metalake_demo", "catalog_hive", "sales", "customers"))
print(gravitino.get_metalakes())
