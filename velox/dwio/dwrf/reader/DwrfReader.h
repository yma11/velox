/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "folly/Executor.h"
#include "folly/synchronization/Baton.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/common/UnitLoader.h"
#include "velox/dwio/dwrf/reader/SelectiveDwrfReader.h"

namespace facebook::velox::dwrf {

class ColumnReader;
class DwrfUnit;

class DwrfRowReader : public StrideIndexProvider,
                      public StripeReaderBase,
                      public dwio::common::RowReader {
 public:
  /**
   * Constructor that lets the user specify additional options.
   * @param contents of the file
   * @param options options for reading
   */
  DwrfRowReader(
      const std::shared_ptr<ReaderBase>& reader,
      const dwio::common::RowReaderOptions& options);

  ~DwrfRowReader() override = default;

  // Select the columns from the options object
  const dwio::common::ColumnSelector& getColumnSelector() const {
    return *columnSelector_;
  }

  const std::shared_ptr<dwio::common::ColumnSelector>& getColumnSelectorPtr()
      const {
    return columnSelector_;
  }

  const dwio::common::RowReaderOptions& getRowReaderOptions() const {
    return options_;
  }

  std::shared_ptr<const dwio::common::TypeWithId> getSelectedType() const {
    if (!selectedSchema_) {
      selectedSchema_ = columnSelector_->buildSelected();
    }

    return selectedSchema_;
  }

  uint64_t getRowNumber() const {
    return previousRow_;
  }

  uint64_t seekToRow(uint64_t rowNumber);

  uint64_t skipRows(uint64_t numberOfRowsToSkip);

  uint32_t getCurrentStripe() const {
    return currentStripe_;
  }

  uint64_t getStrideIndex() const override {
    return strideIndex_;
  }

  // Estimate the space used by the reader
  size_t estimatedReaderMemory() const;

  // Estimate the row size for projected columns
  std::optional<size_t> estimatedRowSize() const override;

  // Returns number of rows read. Guaranteed to be less then or equal to size.
  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const dwio::common::Mutation* = nullptr) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override {
    stats.skippedStrides += skippedStrides_;
    stats.processedStrides += processedStrides_;
    stats.columnReaderStatistics.flattenStringDictionaryValues +=
        columnReaderStatistics_.flattenStringDictionaryValues;
  }

  void resetFilterCaches() override;

  bool allPrefetchIssued() const override {
    return true;
  }

  // Returns the skipped strides for 'stripe'. Used for testing.
  std::optional<std::vector<uint64_t>> stridesToSkip(uint32_t stripe) const {
    auto it = stripeStridesToSkip_.find(stripe);
    if (it == stripeStridesToSkip_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  void loadCurrentStripe();

  std::optional<std::vector<PrefetchUnit>> prefetchUnits() override {
    return std::nullopt;
  }

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

 private:
  // footer
  std::vector<uint64_t> firstRowOfStripe_;
  mutable std::shared_ptr<const dwio::common::TypeWithId> selectedSchema_;

  // reading state
  uint64_t previousRow_;
  uint32_t firstStripe_;
  uint32_t currentStripe_;
  // The the stripe AFTER the last one that should be read. e.g. if the highest
  // stripe in the RowReader's bounds is 3, then stripeCeiling_ is 4.
  uint32_t stripeCeiling_;
  uint64_t currentRowInStripe_;
  uint64_t rowsInCurrentStripe_;
  uint64_t strideIndex_;
  dwio::common::RowReaderOptions options_;
  std::function<void(uint64_t)> decodingTimeUsCallback_;

  // column selector
  std::shared_ptr<dwio::common::ColumnSelector> columnSelector_;

  const uint64_t* stridesToSkip_;
  int stridesToSkipSize_;
  // Record of strides to skip in each visited stripe. Used for diagnostics.
  std::unordered_map<uint32_t, std::vector<uint64_t>> stripeStridesToSkip_;
  // Number of skipped strides.
  int64_t skippedStrides_{0};
  // Number of processed strides.
  int64_t processedStrides_{0};
  
  // Set to true after clearing filter caches, i.e. adding a dynamic
  // filter. Causes filters to be re-evaluated against stride stats on
  // next stride instead of next stripe.
  bool recomputeStridesToSkip_{false};

  dwio::common::ColumnReaderStatistics columnReaderStatistics_;

  bool atEnd_{false};

  std::unique_ptr<dwio::common::UnitLoader> unitLoader_;
  DwrfUnit* currentUnit_;

  // internal methods

  std::optional<size_t> estimatedRowSizeHelper(
      const FooterWrapper& fileFooter,
      const dwio::common::Statistics& stats,
      uint32_t nodeId) const;

  std::shared_ptr<const RowType> getType() const {
    return columnSelector_->getSchema();
  }

  bool isEmptyFile() const {
    return (stripeCeiling_ == firstStripe_);
  }

  void checkSkipStrides(uint64_t strideSize);

  void readNext(
      uint64_t rowsToRead,
      const dwio::common::Mutation*,
      VectorPtr& result);

  void readWithRowNumber(
      uint64_t rowsToRead,
      const dwio::common::Mutation*,
      VectorPtr& result);

  uint64_t skip(uint64_t numValues);

  std::unique_ptr<ColumnReader>& getColumnReader();

  std::unique_ptr<dwio::common::SelectiveColumnReader>&
  getSelectiveColumnReader();

  std::unique_ptr<dwio::common::UnitLoader> getUnitLoader();
};

class DwrfReader : public dwio::common::Reader {
 public:
  /**
   * Constructor that lets the user specify reader options and input stream.
   */
  DwrfReader(
      const dwio::common::ReaderOptions& options,
      std::unique_ptr<dwio::common::BufferedInput> input);

  ~DwrfReader() override = default;

  common::CompressionKind getCompression() const {
    return readerBase_->getCompressionKind();
  }

  WriterVersion getWriterVersion() const {
    return readerBase_->getWriterVersion();
  }

  const std::string& getWriterName() const {
    return readerBase_->getWriterName();
  }

  std::vector<std::string> getMetadataKeys() const;

  std::string getMetadataValue(const std::string& key) const;

  bool hasMetadataValue(const std::string& key) const;

  uint64_t getCompressionBlockSize() const {
    return readerBase_->getCompressionBlockSize();
  }

  uint32_t getNumberOfStripes() const {
    return readerBase_->getFooter().stripesSize();
  }

  std::vector<uint64_t> getRowsPerStripe() const {
    return readerBase_->getRowsPerStripe();
  }
  uint32_t strideSize() const {
    return readerBase_->getFooter().rowIndexStride();
  }

  std::unique_ptr<StripeInformation> getStripe(uint32_t) const;

  uint64_t getFileLength() const {
    return readerBase_->getFileLength();
  }

  std::unique_ptr<dwio::common::Statistics> getStatistics() const {
    return readerBase_->getStatistics();
  }

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t nodeId) const override {
    return readerBase_->getColumnStatistics(nodeId);
  }

  const std::shared_ptr<const RowType>& rowType() const override {
    return readerBase_->getSchema();
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override {
    return readerBase_->getSchemaWithId();
  }

  const PostScript& getPostscript() const {
    return readerBase_->getPostScript();
  }

  const FooterWrapper& getFooter() const {
    return readerBase_->getFooter();
  }

  std::optional<uint64_t> numberOfRows() const override {
    auto& fileFooter = readerBase_->getFooter();
    if (fileFooter.hasNumberOfRows()) {
      return fileFooter.numberOfRows();
    }
    return std::nullopt;
  }

  static uint64_t getMemoryUse(
      ReaderBase& readerBase,
      int32_t stripeIx,
      const dwio::common::ColumnSelector& cs);

  uint64_t getMemoryUse(int32_t stripeIx = -1);

  uint64_t getMemoryUseByFieldId(
      const std::vector<uint64_t>& include,
      int32_t stripeIx = -1);

  uint64_t getMemoryUseByName(
      const std::vector<std::string>& names,
      int32_t stripeIx = -1);

  uint64_t getMemoryUseByTypeId(
      const std::vector<uint64_t>& include,
      int32_t stripeIx = -1);

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override;

  std::unique_ptr<DwrfRowReader> createDwrfRowReader(
      const dwio::common::RowReaderOptions& options = {}) const;

  /**
   * Create a reader to the for the dwrf file.
   * @param stream the stream to read
   * @param options the options for reading the file
   */
  static std::unique_ptr<DwrfReader> create(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options);

  ReaderBase* testingReaderBase() const {
    return readerBase_.get();
  }

 private:
  // Ensures that files column names match the ones from the table schema using
  // column indices.
  void updateColumnNamesFromTableSchema();

 private:
  std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::ReaderOptions options_;
};

class DwrfReaderFactory : public dwio::common::ReaderFactory {
 public:
  DwrfReaderFactory() : ReaderFactory(dwio::common::FileFormat::DWRF) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options) override {
    return DwrfReader::create(std::move(input), options);
  }
};

class OrcReaderFactory : public dwio::common::ReaderFactory {
 public:
  OrcReaderFactory() : ReaderFactory(dwio::common::FileFormat::ORC) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options) override {
    return DwrfReader::create(std::move(input), options);
  }
};

void registerDwrfReaderFactory();

void unregisterDwrfReaderFactory();

void registerOrcReaderFactory();

void unregisterOrcReaderFactory();

} // namespace facebook::velox::dwrf
