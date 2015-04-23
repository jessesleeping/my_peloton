/*-------------------------------------------------------------------------
 *
 * tile.h
 * the base class for all tiles
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/tile.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "catalog/manager.h"
#include "catalog/schema.h"
#include "storage/backend.h"
#include "storage/tuple.h"
#include "storage/tile_group_header.h"
#include "storage/backend.h"
#include "storage/vm_backend.h"

#include <mutex>

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tile
//===--------------------------------------------------------------------===//

class TileIterator;
//class TileStats;

/**
 * Represents a Tile.
 *
 * Tiles are only instantiated via TileFactory.
 *
 * NOTE: MVCC is implemented on the shared TileGroupHeader.
 */
class Tile {
  friend class TileFactory;
  friend class TileIterator;
  friend class TileGroupHeader;


  Tile() = delete;
  Tile(Tile const&) = delete;

 public:

  // Tile creator
  Tile(TileGroupHeader* tile_header,
       Backend* backend,
       const catalog::Schema& tuple_schema,
       int tuple_count);

  virtual ~Tile();

  //===--------------------------------------------------------------------===//
  // Operations
  //===--------------------------------------------------------------------===//

  /**
   * Insert tuple at slot
   * NOTE : No checks, must be at valid slot.
   */
  void InsertTuple(const id_t tuple_slot_id, Tuple *tuple);

  // allocated tuple slots
  id_t GetAllocatedTupleCount() const {
    return num_tuple_slots;
  }

  // active tuple slots
  inline virtual id_t GetActiveTupleCount() const {
    return tile_group_header->GetActiveTupleCount();
  }

  int GetTupleOffset(const char *tuple_address) const;

  int GetColumnOffset(const std::string &name) const;

  /**
   * Returns tuple present at slot
   * NOTE : No checks, must be at valid slot and must exist.
   */
  Tuple *GetTuple(const id_t tuple_slot_id);

  /**
   * Returns value present at slot
   * NOTE : No checks, must be at valid slot and must exist.
   */
  Value GetValue(const id_t tuple_slot_id, const id_t column_id);

  void SetValue(Value value, const id_t tuple_slot_id, const id_t column_id);

  // Get tuple at location
  static Tuple *GetTuple(catalog::Manager* catalog, const ItemPointer* tuple_location);

  //===--------------------------------------------------------------------===//
  // Size Stats
  //===--------------------------------------------------------------------===//

  // Only inlined data
  uint32_t GetInlinedSize() const {
    return tile_size ;
  }

  int64_t GetUninlinedDataSize() const {
    return uninlined_data_size;
  }

  // Both inlined and uninlined data
  uint32_t GetSize() const {
    return tile_size + uninlined_data_size ;
  }

  //===--------------------------------------------------------------------===//
  // Columns
  //===--------------------------------------------------------------------===//

  const catalog::Schema& GetSchema() const {
    return schema;
  };

  const std::string GetColumnName(const id_t column_index) const {
    return schema.GetColumnInfo(column_index).name;
  }

  int GetColumnCount() const {
    return column_count;
  };

  TileGroupHeader *GetHeader() const{
    return tile_group_header;
  }

  Backend *GetBackend() const {
    return backend;
  }

  id_t GetTileId() const {
    return tile_id;
  }

  // Compare two tiles
  bool operator== (const Tile &other) const;
  bool operator!= (const Tile &other) const;

  TileIterator GetIterator();

  // Get a string representation of this tile
  friend std::ostream& operator<<(std::ostream& os, const Tile& tile);

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  //===--------------------------------------------------------------------===//

  bool SerializeTo(SerializeOutput &output, id_t num_tuples);
  bool SerializeHeaderTo(SerializeOutput &output);
  bool SerializeTuplesTo(SerializeOutput &output, Tuple *tuples, int num_tuples);

  void DeserializeTuplesFrom(SerializeInput &serialize_in, Pool *pool = nullptr);
  void DeserializeTuplesFromWithoutHeader(SerializeInput &input, Pool *pool = nullptr);

  Pool *GetPool(){
    return (pool);
  }

  char *GetTupleLocation(const id_t tuple_slot_id) const;

 protected:

  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // Catalog information
  id_t database_id;
  id_t table_id;
  id_t tile_group_id;
  id_t tile_id;

  // storage backend
  Backend *backend;

  // tile schema
  catalog::Schema schema;

  // set of fixed-length tuple slots
  char *data;

  // storage pool for uninlined data
  Pool *pool;

  // number of tuple slots allocated
  id_t num_tuple_slots;

  // number of columns
  id_t column_count;

  // length of tile tuple
  size_t tuple_length;

  // space occupied by inlined data (tile size)
  size_t tile_size;

  // space occupied by uninlined data
  size_t uninlined_data_size;

  // Used for serialization/deserialization
  char *column_header;

  id_t column_header_size;

  /**
   * NOTE : Tiles don't keep track of number of occupied slots.
   * This is maintained by shared Tile Header.
   */
  TileGroupHeader *tile_group_header;

  // clean up later
  bool own_tile = false;
};

// Returns a pointer to the tuple requested. No checks are done that the index is valid.
inline char *Tile::GetTupleLocation(const id_t tuple_slot_id) const {
  char *tuple_location = data + (tuple_slot_id * tuple_length);

  return tuple_location;
}

// Finds index of tuple for a given tuple address.
// Returns -1 if no matching tuple was found
inline int Tile::GetTupleOffset(const char* tuple_address) const{

  // check if address within tile bounds
  if((tuple_address < data) || (tuple_address >= (data + tile_size)))
    return -1;

  int tuple_id = 0;

  // check if address is at an offset that is an integral multiple of tuple length
  tuple_id = (tuple_address - data)/tuple_length;

  if(tuple_id * tuple_length + data == tuple_address)
    return tuple_id;

  return -1;
}

inline Tuple *Tile::GetTuple(catalog::Manager* catalog, const ItemPointer* tuple_location) {

  // Figure out tile location
  storage::Tile *tile = (storage::Tile *) catalog->locator[tuple_location->block];
  assert(tile);

  // Look up tuple at tile
  storage::Tuple *tile_tuple = tile->GetTuple(tuple_location->offset);
  return tile_tuple;
}


//===--------------------------------------------------------------------===//
// Tile factory
//===--------------------------------------------------------------------===//

class TileFactory {
 public:
  TileFactory();
  virtual ~TileFactory();

  // This is a temporary function that leaks memory. We have to
  // refactor Tile in order to implement "free" tiles that are used by
  // the executor.
  static Tile *GetTempTile(const catalog::Schema &schema, int tuple_count) {

    Backend *backend = new VMBackend();
    TileGroupHeader *header = nullptr; // No MVCC for these temporary tiles

    Tile *tile = GetTile(
        INVALID_ID, INVALID_ID, INVALID_ID, INVALID_ID,
        header, backend,
        schema,
        tuple_count);

    tile->own_tile = true;

    return tile;
  }

  static Tile *GetTile(id_t database_id, id_t table_id, id_t tile_group_id, id_t tile_id,
                       TileGroupHeader* tile_header,
                       Backend* backend,
                       const catalog::Schema& schema,
                       int tuple_count) {

    Tile *tile = new Tile(tile_header, backend, schema, tuple_count);

    TileFactory::InitCommon(tile, database_id, table_id, tile_group_id, tile_id,
                            backend, schema);

    return tile;
  }

 private:

  static void InitCommon(Tile *tile,
                         id_t database_id, id_t table_id, id_t tile_group_id, id_t tile_id,
                         Backend* backend,
                         const catalog::Schema& schema) {

    tile->database_id = database_id;
    tile->table_id = table_id;
    tile->tile_group_id = tile_group_id;
    tile->tile_id = tile_id;
    tile->backend = backend;
    tile->schema = schema;
  }

};


} // End storage namespace
} // End nstore namespace
