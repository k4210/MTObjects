#pragma once

#include <iterator>
#include <array>
#include <bitset>
#include <intrin.h> 
#include <cstring>
#include <algorithm>

#ifndef TEST_STUFF
//#define TEST_STUFF
#endif

#ifdef TEST_STUFF 
#include <assert.h>
#define Assert assert
#else
#define Assert(expression) ((void)0)
#endif //TEST_STUFF

namespace MTObjects
{
#ifdef TEST_STUFF 
	struct TestStuff
	{
		static unsigned int cluster_in_obj_overwritten;
		static unsigned int max_num_objects_to_handle;
		static std::size_t max_num_clusters;
		static unsigned int max_num_data_chunks_used;
	};
#endif //TEST_STUFF

	namespace SmartStackStuff
	{
		static const constexpr unsigned int kCacheLineSize = 64; // == 16 * sizeof(int) == 8 * sizeof(*int)
		static const constexpr unsigned int kCacheLinesPerChunk = 48;
		struct DataChunk
		{
			static const constexpr unsigned int kStoragePerChunk = (kCacheLinesPerChunk * kCacheLineSize) - (2 * sizeof(DataChunk*));

			DataChunk* previous_chunk_ = nullptr;
			DataChunk* next_chunk_ = nullptr;
			unsigned char memory_[kStoragePerChunk];

			void Clear()
			{
				previous_chunk_ = nullptr;
				next_chunk_ = nullptr;
#ifdef TEST_STUFF 
				std::memset(memory_, 0xEEEE, kStoragePerChunk);
#endif //TEST_STUFF
			}

			unsigned char* GetMemory()
			{
				return memory_;
			}
		};
		static_assert(sizeof(DataChunk) == (kCacheLineSize * kCacheLinesPerChunk));

		struct DataChunkMemoryPool64
		{
			static const constexpr int kBitsetSize = 64;
			static const constexpr int kRangeNum = 128;
			static const constexpr int kNumberChunks = kBitsetSize * kRangeNum;

			struct ExtendedBitset
			{
				std::array<std::bitset<kBitsetSize>, 2> bitsets_;

				static bool FirstZeroInBitset(const std::bitset<kBitsetSize>& bitset, unsigned long& out_index)
				{
					return 0 != _BitScanForward64(&out_index, ~bitset.to_ullong());
				}

				unsigned int FirstZeroIndex() const
				{
					unsigned long result1 = 0;
					const bool ok1 = FirstZeroInBitset(bitsets_[0], result1);

					unsigned long result2 = 0;
					const bool ok2 = FirstZeroInBitset(bitsets_[1], result2);
					result2 += kBitsetSize;

					Assert(ok1 || ok2);

					return ok1 ? result1 : result2;
				}

				bool Test(unsigned int i) const
				{
					return bitsets_[i / kBitsetSize][i % kBitsetSize];
				}

				void Set(unsigned int i, bool value)
				{
					bitsets_[i / kBitsetSize][i % kBitsetSize] = value;
				}
			};

		private:
			std::array<DataChunk, kNumberChunks> chunks_;
			std::array<std::bitset<kBitsetSize>, kRangeNum> is_element_occupied_;
			ExtendedBitset is_range_occupied_;
#ifdef TEST_STUFF 
			unsigned int num_chunks_allocated = 0;
#endif //TEST_STUFF
		public:
			static DataChunkMemoryPool64 instance;

			DataChunkMemoryPool64() = default;
			~DataChunkMemoryPool64() = default;
			DataChunkMemoryPool64(DataChunkMemoryPool64&) = delete;
			DataChunkMemoryPool64& operator=(DataChunkMemoryPool64&) = delete;

			static unsigned int FirstZeroInBitset(const std::bitset<kBitsetSize>& bitset)
			{
				unsigned long result = 0xFFFFFFFF;
				const bool ok = ExtendedBitset::FirstZeroInBitset(bitset, result);
				Assert(ok);
				return result;
			}

			DataChunk* Allocate()
			{
				const auto first_range_with_free_space = is_range_occupied_.FirstZeroIndex();
				Assert(first_range_with_free_space < kRangeNum);
				auto& range_bitset = is_element_occupied_[first_range_with_free_space];

				Assert(!range_bitset.all());
				const auto bit_idx = FirstZeroInBitset(range_bitset);

				Assert(!range_bitset[bit_idx]);
				range_bitset[bit_idx] = true;

				is_range_occupied_.Set(first_range_with_free_space, range_bitset.all());

#ifdef TEST_STUFF 
				num_chunks_allocated++;
				TestStuff::max_num_data_chunks_used = std::max<unsigned int>(TestStuff::max_num_data_chunks_used, num_chunks_allocated);
#endif //TEST_STUFF

				return &chunks_[first_range_with_free_space * kBitsetSize + bit_idx];
			}

			void Release(DataChunk* chunk)
			{
				const unsigned int index = static_cast<unsigned int>(std::distance(chunks_.data(), chunk));
				Assert(index >= 0 && index < kNumberChunks);

				const auto range = index / kBitsetSize;
				const auto bit_idx = index % kBitsetSize;

				auto& range_bitset = is_element_occupied_[range];
				Assert(range_bitset[bit_idx]);
				range_bitset[bit_idx] = false;

				is_range_occupied_.Set(range, false);

#ifdef TEST_STUFF 
				num_chunks_allocated--;
#endif //TEST_STUFF
			}
		};
	};

	/*
	SmartStack is optimized for:
	- push_back, back, pop_back
	- unordered merge
	*/
	template<typename T>
	struct SmartStack
	{
		static_assert(SmartStackStuff::DataChunk::kStoragePerChunk >= sizeof(T), "too big T");
		static const constexpr int kElementsPerChunk = SmartStackStuff::DataChunk::kStoragePerChunk / sizeof(T);

		SmartStackStuff::DataChunk* first_chunk_ = nullptr;
		SmartStackStuff::DataChunk* last_chunk_ = nullptr;
		int number_chunks_ = 0;
		int number_of_elements_in_last_chunk_ = kElementsPerChunk;

	private:
		void AllocateNextChunk()
		{
			auto new_chunk = SmartStackStuff::DataChunkMemoryPool64::instance.Allocate();
			Assert(new_chunk);
			new_chunk->Clear();

			number_chunks_++;
			Assert(kElementsPerChunk == number_of_elements_in_last_chunk_);
			number_of_elements_in_last_chunk_ = 0;

			if (last_chunk_)
			{
				Assert(nullptr == last_chunk_->next_chunk_);
				last_chunk_->next_chunk_ = new_chunk;
				new_chunk->previous_chunk_ = last_chunk_;
				last_chunk_ = new_chunk;
			}
			else
			{
				Assert(nullptr == first_chunk_);
				last_chunk_ = first_chunk_ = new_chunk;
			}
		}

		void ReleaseLastChunk()
		{
			auto chunk_to_release = last_chunk_;
			Assert(chunk_to_release);

			number_chunks_--;
			Assert(0 == number_of_elements_in_last_chunk_);
			number_of_elements_in_last_chunk_ = kElementsPerChunk;
			SmartStackStuff::DataChunkMemoryPool64::instance.Release(chunk_to_release);

			last_chunk_ = chunk_to_release->previous_chunk_;
			if (last_chunk_)
			{
				last_chunk_->next_chunk_ = nullptr;
			}
			else
			{
				Assert(chunk_to_release == first_chunk_);
				Assert(0 == number_chunks_);
				first_chunk_ = nullptr;
			}
		}

		T* ElementsInLastChunk() const
		{
			Assert(last_chunk_);
			return reinterpret_cast<T*>(last_chunk_->GetMemory());
		}
	public:
		unsigned int size() const
		{
			return (number_chunks_ - 1) * kElementsPerChunk + number_of_elements_in_last_chunk_;
		}

		bool empty() const
		{
			return 0 == size();
		}

		T& back()
		{
			return ElementsInLastChunk()[number_of_elements_in_last_chunk_ - 1];
		}

		const T& back() const
		{

			return ElementsInLastChunk()[number_of_elements_in_last_chunk_ - 1];
		}

		void push_back(const T& value)
		{
			if (kElementsPerChunk == number_of_elements_in_last_chunk_)
			{
				AllocateNextChunk();
			}
			new (ElementsInLastChunk() + number_of_elements_in_last_chunk_) T(value);
			number_of_elements_in_last_chunk_++;
		}

		void pop_back()
		{
			Assert(!empty());
			number_of_elements_in_last_chunk_--;
			(ElementsInLastChunk() + number_of_elements_in_last_chunk_)->~T();
#ifdef TEST_STUFF 
			std::memset((ElementsInLastChunk() + number_of_elements_in_last_chunk_), 0xEEEE, sizeof(T));
#endif //TEST_STUFF
			if (0 == number_of_elements_in_last_chunk_)
			{
				ReleaseLastChunk();
			}
		}

		void clear()
		{
			if constexpr (std::is_pod<T>::value)
			{
				for (auto chunk = first_chunk_; chunk;)
				{
					auto temo_ptr = chunk;
					chunk = chunk->next_chunk_;
					SmartStackStuff::DataChunkMemoryPool64::instance.Release(temo_ptr);
				}
				first_chunk_ = nullptr;
				last_chunk_ = nullptr;
				number_chunks_ = 0;
				number_of_elements_in_last_chunk_ = kElementsPerChunk;
			}
			else
			{
				while (number_chunks_)
				{
					pop_back();
				}
				Assert(first_chunk_ == nullptr);
				Assert(last_chunk_ == nullptr);
				Assert(number_chunks_ == 0);
				Assert(number_of_elements_in_last_chunk_ == kElementsPerChunk);
			}
		}

	public:
		struct Iter : public std::iterator<std::bidirectional_iterator_tag, T>
		{
		private:
			SmartStackStuff::DataChunk* chunk_ = nullptr;
			int element_index_ = 0;

			T* GetChunkElements() const
			{
				Assert(chunk_);
				return reinterpret_cast<T*>(chunk_->GetMemory());
			}

			void Increment()
			{
				Assert(chunk_);
				if ((SmartStack::kElementsPerChunk - 1) == element_index_)
				{
					chunk_ = chunk_->next_chunk_;
					element_index_ = 0;
				}
				else
				{
					element_index_++;
					Assert(element_index_ >= 0 && element_index_ < SmartStack::kElementsPerChunk);
				}
			}

			void Decrement()
			{
				Assert(chunk_);
				if (0 == element_index_)
				{
					chunk_ = chunk_->previous_chunk_;
					Assert(chunk_);
					element_index_ = SmartStack::kElementsPerChunk - 1;
				}
				else
				{
					element_index_--;
					Assert(element_index_ >= 0 && element_index_ < SmartStack::kElementsPerChunk);
				}
			}
		public:
			Iter() = default;
			Iter(SmartStackStuff::DataChunk* in_chunk, int in_element_index)
				: chunk_(in_chunk), element_index_(in_element_index)
			{
				Assert(chunk_ || !element_index_);
				Assert(element_index_ >= 0 && element_index_ < SmartStack::kElementsPerChunk);
			}
			Iter(const Iter& other) = default;
			Iter& operator=(const Iter& other) = default;

			Iter& operator++()
			{
				Increment();
				return *this;
			}

			Iter operator++(int)
			{
				Iter result(*this);
				Increment();
				return result;
			}

			Iter& operator--()
			{
				Decrement();
				return *this;
			}

			Iter operator--(int)
			{
				Iter result(*this);
				Decrement();
				return result;
			}

			T& operator*() const
			{
				return GetChunkElements()[element_index_];
			}

			T& operator->() const
			{
				return GetChunkElements()[element_index_];
			}

			bool operator ==(const Iter& other) const
			{
				return chunk_ == other.chunk_
					&& element_index_ == other.element_index_;
			}

			bool operator !=(const Iter& other) const
			{
				return chunk_ != other.chunk_
					|| element_index_ != other.element_index_;
			}
		};

		Iter begin() const
		{
			return Iter(first_chunk_, 0);
		}

		Iter end() const
		{
			Assert(!empty() || first_chunk_ == nullptr);
			Assert(!empty() || last_chunk_ == nullptr);

			if (empty() || (number_of_elements_in_last_chunk_ == SmartStack::kElementsPerChunk))
			{
				return Iter(nullptr, 0);
			}
			return Iter(last_chunk_, number_of_elements_in_last_chunk_);
		}

	public:
		SmartStack() = default;
		~SmartStack()
		{
			clear();
		}

		static void UnorderedMerge(SmartStack& dst, SmartStack& src)
		{
			if (src.empty()) { return; }
			if (dst.empty())
			{
				// Move everything
				dst.first_chunk_ = src.first_chunk_;
				dst.last_chunk_ = src.last_chunk_;
				dst.number_chunks_ = src.number_chunks_;
				dst.number_of_elements_in_last_chunk_ = src.number_of_elements_in_last_chunk_;
			}
			else
			{
				// Move some elements from last chunk
				const int num_free_slots_in_last_chunk_dst = kElementsPerChunk - dst.number_of_elements_in_last_chunk_;
				const int num_elements_to_move = std::min(num_free_slots_in_last_chunk_dst, src.number_of_elements_in_last_chunk_);
				if constexpr(std::is_pod<T>::value)
				{
					if (num_elements_to_move)
					{
						const unsigned int remaining_elements_in_last_chunk_src = src.number_of_elements_in_last_chunk_ - num_elements_to_move;
						std::memcpy(&dst.ElementsInLastChunk()[dst.number_of_elements_in_last_chunk_]
							, &src.ElementsInLastChunk()[remaining_elements_in_last_chunk_src]
							, num_elements_to_move * sizeof(T));
						dst.number_of_elements_in_last_chunk_ += num_elements_to_move;
						src.number_of_elements_in_last_chunk_ -= num_elements_to_move;
						if (0 == src.number_of_elements_in_last_chunk_)
						{
							src.ReleaseLastChunk();
						}
#ifdef TEST_STUFF 
						else
						{
							std::memset(&src.ElementsInLastChunk()[remaining_elements_in_last_chunk_src], 0xEEEE, sizeof(T) * (kElementsPerChunk - remaining_elements_in_last_chunk_src));
						}
#endif //TEST_STUFF
					}
				}
				else
				{
					for (int i = 0; i < num_elements_to_move; i++)
					{
						dst.push_back(src.back());
						src.pop_back();
					}
				}

				if (src.empty()) { return; }
				
				//Re-bind chunks
				//last chunk in dst or in src is full
				Assert(dst.number_of_elements_in_last_chunk_ == kElementsPerChunk || src.number_of_elements_in_last_chunk_ == kElementsPerChunk);
				if (dst.number_of_elements_in_last_chunk_ == kElementsPerChunk)
				{	//src goes last
					dst.last_chunk_->next_chunk_ = src.first_chunk_;
					src.first_chunk_->previous_chunk_ = dst.last_chunk_;
					dst.last_chunk_ = src.last_chunk_;

					dst.number_of_elements_in_last_chunk_ = src.number_of_elements_in_last_chunk_;
				}
				else
				{	//src goes first
					src.last_chunk_->next_chunk_ = dst.first_chunk_;
					dst.first_chunk_->previous_chunk_ = src.last_chunk_;
					dst.first_chunk_ = src.first_chunk_;
				}
				dst.number_chunks_ += src.number_chunks_;
			}
			src.first_chunk_ = nullptr;
			src.last_chunk_ = nullptr;
			src.number_chunks_ = 0;
			src.number_of_elements_in_last_chunk_ = kElementsPerChunk;
		}

		template<class It>
		void Insert(It begin, It end)
		{
			for (; begin != end; ++begin)
			{
				push_back(*begin);
			}
		}
	};

}